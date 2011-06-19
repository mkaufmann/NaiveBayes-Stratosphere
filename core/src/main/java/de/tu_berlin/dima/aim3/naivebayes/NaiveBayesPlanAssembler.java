package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Collection;
import java.util.LinkedList;

import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.IdfCalculator;
import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.TfIdfCalculator;
import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.TotalDistinctFeatureCountMapper;
import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.TotalDistinctFeatureCountReducer;
import de.tu_berlin.dima.aim3.naivebayes.classifier.PactBayesDatastore;
import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureCountPair;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.data.TfList;
import de.tu_berlin.dima.aim3.naivebayes.data.ThetaNormalizerFactors;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.NaiveBayesDataInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.FeatureSumOutputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.IdfOutputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.LabelSumOutputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.ThetaNormalizedOutputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.TotalSumOutputFormat;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;

public class NaiveBayesPlanAssembler implements PlanAssembler{
	
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : -1);
		String dataInput = (args.length > 1 ? args[1] : "file:///home/mkaufmann/datasets/train");
		String dataOutput    = (args.length > 2 ? args[2] : "file:///home/mkaufmann/datasets/result");
		
		int numLabels = 20; //Distinct labels
		int numFeatures = 150000; //Distinct features
		int avgFeaturesPerDocument = 300;
		int avgFeaturesPerLabel = 15000; //Avg distinct features per label
		
		int doubleSize = 8;
		int intSize = 4;
		int labelSize = 16; 
		int featureSize = 16;
		
		String idfOutputPath = dataOutput + PactBayesDatastore.WEIGHT_DEFAULT_PATH;
		String thetaNormalizerOutputPath = dataOutput + PactBayesDatastore.THETA_NORMALIZER_DEFAULT_PATH;
		String sigmaJOutputPath = dataOutput + PactBayesDatastore.SIGMA_J_DEFAULT_PATH;
		String sigmaKOutputPath = dataOutput + PactBayesDatastore.SIGMA_K_DEFAULT_PATH;
		String sigmaKSigmaJOutputPath = dataOutput + PactBayesDatastore.SIGMA_K_SIGMA_J_DEFAULT_PATH;
		
		DataSourceContract<Label, FeatureList> source = new DataSourceContract<Label, FeatureList>
			(NaiveBayesDataInputFormat.class, dataInput, "Naive Bayes Input");
		source.setDegreeOfParallelism(noSubTasks);
		source.getCompilerHints().setAvgBytesPerRecord(1500);
		
		MapContract<Label, FeatureList, Label, TfList> featureBaseMapper = 
			new MapContract<Label, FeatureList, Label, TfList>(BayesFeatureMapper.Base.class, "Feature/Token generator");
		featureBaseMapper.setDegreeOfParallelism(noSubTasks);
		featureBaseMapper.setInput(source);
		featureBaseMapper.getCompilerHints().setKeyCardinality(numLabels);
		setCompilerHints(featureBaseMapper, numLabels, 1, labelSize + avgFeaturesPerDocument*(featureSize+intSize), -1);
		
		MapContract<Label, TfList, Feature, PactInteger> featureCountMapper = 
			new MapContract<Label, TfList, Feature, PactInteger>(BayesFeatureMapper.DistinctFeatureCount.class, "Feature Count Mapper");
		featureCountMapper.setDegreeOfParallelism(noSubTasks);
		featureCountMapper.setInput(featureBaseMapper);
		setCompilerHints(featureCountMapper, numFeatures, avgFeaturesPerDocument, featureSize+intSize, -1);
		
		ReduceContract<Feature, PactInteger, Feature, PactInteger> featureCountReducer = 
			new ReduceContract<Feature, PactInteger, Feature, PactInteger>(BayesFeatureReducer.FeatureCount.class, "Feature Count Reducer");
		featureCountReducer.setDegreeOfParallelism(noSubTasks);
		featureCountReducer.setInput(featureCountMapper);
		setCompilerHints(featureCountReducer, numFeatures, 1, labelSize+intSize, 1);
		
		MapContract<Label, TfList, Label, PactInteger> labelDocumentCountMapper = 
			new MapContract<Label, TfList, Label, PactInteger>(BayesFeatureMapper.LabelCount.class, "Label Document Count (M)");
		labelDocumentCountMapper.setDegreeOfParallelism(noSubTasks);
		labelDocumentCountMapper.setInput(featureBaseMapper);
		setCompilerHints(labelDocumentCountMapper, numLabels, 1, labelSize+intSize, 1);
		
		ReduceContract<Label, PactInteger, Label, PactInteger> labelDocumentCountReducer = 
			new ReduceContract<Label, PactInteger, Label, PactInteger>(BayesFeatureReducer.LabelCount.class, "Label Document Count (R)");
		labelDocumentCountReducer.setDegreeOfParallelism(noSubTasks);
		labelDocumentCountReducer.setInput(labelDocumentCountMapper);
		setCompilerHints(labelDocumentCountReducer, numLabels, 1, labelSize+intSize, 1);
		
		MapContract<Label, TfList, Feature, PactInteger> featureTfMapper = 
			new MapContract<Label, TfList, Feature, PactInteger>(BayesFeatureMapper.FeatureTf.class, "Tf (M)");
		featureTfMapper.setDegreeOfParallelism(noSubTasks);
		featureTfMapper.setInput(featureBaseMapper);
		setCompilerHints(featureTfMapper, numFeatures, avgFeaturesPerDocument, featureSize+intSize, -1);
		
		ReduceContract<Feature, PactInteger, Feature, PactInteger> featureTfReducer = 
			new ReduceContract<Feature, PactInteger, Feature, PactInteger>(BayesFeatureReducer.FeatureTf.class, "Tf (R)");
		featureTfReducer.setDegreeOfParallelism(noSubTasks);
		featureTfReducer.setInput(featureTfMapper);
		setCompilerHints(featureTfReducer, numFeatures, 1, featureSize+intSize, 1);
		
		MapContract<Label, TfList, LabelFeaturePair, PactInteger> dfMapper = 
			new MapContract<Label, TfList, LabelFeaturePair, PactInteger>(BayesFeatureMapper.DocumentFrequency.class, "Df (M)");
		dfMapper.setDegreeOfParallelism(noSubTasks);
		dfMapper.setInput(featureBaseMapper);
		setCompilerHints(dfMapper, numLabels*avgFeaturesPerLabel, avgFeaturesPerDocument, labelSize+featureSize+intSize, -1);
		
		ReduceContract<LabelFeaturePair, PactInteger, Label, FeatureCountPair> dfReducer = 
			new ReduceContract<LabelFeaturePair, PactInteger, Label, FeatureCountPair>(BayesFeatureReducer.DocumentFrequency.class, "Df (R)");
		dfReducer.setDegreeOfParallelism(noSubTasks);
		dfReducer.setInput(dfMapper);
		dfReducer.getCompilerHints().setKeyCardinality(numFeatures);
		setCompilerHints(dfReducer, numLabels, 1, labelSize+featureSize+doubleSize, -1);
		
		MapContract<Label, TfList, LabelFeaturePair, PactDouble> normalizedTfMapper = 
			new MapContract<Label, TfList, LabelFeaturePair, PactDouble>(BayesFeatureMapper.NormalizedTf.class, "Normalized Tf (M)");
		normalizedTfMapper.setDegreeOfParallelism(noSubTasks);
		normalizedTfMapper.setInput(featureBaseMapper);
		setCompilerHints(normalizedTfMapper, numLabels*avgFeaturesPerDocument, avgFeaturesPerDocument, labelSize+featureSize+doubleSize, -1);
		
		ReduceContract<LabelFeaturePair, PactDouble, LabelFeaturePair, PactDouble> normalizedTfReducer = 
			new ReduceContract<LabelFeaturePair, PactDouble, LabelFeaturePair, PactDouble>(BayesFeatureReducer.NormalizedTf.class, "Normalized Tf (R)");
		normalizedTfReducer.setDegreeOfParallelism(noSubTasks);
		normalizedTfReducer.setInput(normalizedTfMapper);
		setCompilerHints(normalizedTfReducer, numLabels*avgFeaturesPerLabel, 1, labelSize+featureSize+doubleSize, 1);
		
		MatchContract<Label, PactInteger, FeatureCountPair, LabelFeaturePair, PactDouble> idfCalculatorMatcher =
			new MatchContract<Label, PactInteger, FeatureCountPair, LabelFeaturePair, PactDouble>(IdfCalculator.class, "Idf Calculator");
		idfCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		idfCalculatorMatcher.setFirstInput(labelDocumentCountReducer); //trainerDocCount
		idfCalculatorMatcher.setSecondInput(dfReducer); //documentFrequency (trainer-termDocCount)
		setCompilerHints(idfCalculatorMatcher, numLabels*avgFeaturesPerLabel, 1, labelSize+featureSize+doubleSize, -1);
		
		MatchContract<LabelFeaturePair, PactDouble, PactDouble, LabelFeaturePair, PactDouble> tfIdfCalculatorMatcher = 
			new MatchContract<LabelFeaturePair, PactDouble, PactDouble, LabelFeaturePair, PactDouble>(TfIdfCalculator.class, "TfIdf Calculator");
		tfIdfCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		tfIdfCalculatorMatcher.setFirstInput(idfCalculatorMatcher);
		tfIdfCalculatorMatcher.setSecondInput(normalizedTfReducer); //weight (trainer-wordFreq)
		setCompilerHints(tfIdfCalculatorMatcher, numLabels*avgFeaturesPerLabel, 1, labelSize+featureSize+doubleSize, 1);
		
		MapContract<LabelFeaturePair, PactDouble, Feature, PactDouble> featureSummerMapper = 
			new MapContract<LabelFeaturePair, PactDouble, Feature, PactDouble>(BayesWeightMapper.FeatureSummer.class, "Feature Idf Summer Mapper");
		featureSummerMapper.setDegreeOfParallelism(noSubTasks);
		featureSummerMapper.setInput(tfIdfCalculatorMatcher);
		setCompilerHints(featureSummerMapper, numLabels, 1, featureSize, -1);
		
		ReduceContract<Feature, PactDouble, Feature, PactDouble> featureSummerReducer = 
			new ReduceContract<Feature, PactDouble, Feature, PactDouble>(BayesWeightReducer.FeatureSummer.class, "Feature Idf Summer Reducer");
		featureSummerReducer.setDegreeOfParallelism(noSubTasks);
		featureSummerReducer.setInput(featureSummerMapper);
		setCompilerHints(featureSummerReducer, numFeatures, 1, featureSize+doubleSize, 1);
		
		MapContract<LabelFeaturePair, PactDouble, Label, PactDouble> labelSummerMapper = 
			new MapContract<LabelFeaturePair, PactDouble, Label, PactDouble>(BayesWeightMapper.LabelSummer.class, "Weight Idf Summer Mapper");
		labelSummerMapper.setDegreeOfParallelism(noSubTasks);
		labelSummerMapper.setInput(tfIdfCalculatorMatcher);
		setCompilerHints(labelSummerMapper, numLabels, 1, labelSize+doubleSize, -1);
		
		ReduceContract<Label, PactDouble, Label, PactDouble> labelSummerReducer = 
			new ReduceContract<Label, PactDouble, Label, PactDouble>(BayesWeightReducer.LabelSummer.class, "Weight Idf Summer Reducer");
		labelSummerReducer.setDegreeOfParallelism(noSubTasks);
		labelSummerReducer.setInput(labelSummerMapper);
		setCompilerHints(labelSummerReducer, numLabels, 1, labelSize+doubleSize, numLabels);
		
		MapContract<LabelFeaturePair, PactDouble, PactNull, PactDouble> totalSummerMapper = 
			new MapContract<LabelFeaturePair, PactDouble, PactNull, PactDouble>(BayesWeightMapper.TotalSummer.class, "Total Sum Mapper");
		totalSummerMapper.setDegreeOfParallelism(noSubTasks);
		totalSummerMapper.setInput(tfIdfCalculatorMatcher);
		setCompilerHints(totalSummerMapper, 1, 1, 4+doubleSize, -1);
		
		ReduceContract<PactNull, PactDouble, PactNull, PactDouble> totalSummerReducer = 
			new ReduceContract<PactNull, PactDouble, PactNull, PactDouble>(BayesWeightReducer.NullSummer.class, "Total Sum Reducer");
		totalSummerReducer.setDegreeOfParallelism(1);
		totalSummerReducer.setInput(totalSummerMapper);
		setCompilerHints(totalSummerReducer, 1, 1, 4+doubleSize, 1);
		
		MapContract<LabelFeaturePair, PactDouble, Label, PactDouble> tfidfTransformMapper = 
			new MapContract<LabelFeaturePair, PactDouble, Label, PactDouble>(BayesThetaNormalizer.TfIdfTransform.class, "Tfidf Label Extractor");
		tfidfTransformMapper.setDegreeOfParallelism(noSubTasks);
		tfidfTransformMapper.setInput(tfIdfCalculatorMatcher);
		setCompilerHints(tfidfTransformMapper, numLabels, 1, labelSize+doubleSize, -1);
		
		MapContract<Feature, PactInteger, PactNull, PactInteger> totalDistinctFeatureCountMapper =
			new MapContract<Feature, PactInteger, PactNull, PactInteger>(TotalDistinctFeatureCountMapper.class, "Total distinct feature count (M)");
		totalDistinctFeatureCountMapper.setDegreeOfParallelism(noSubTasks);
		totalDistinctFeatureCountMapper.setInput(featureCountReducer); //trainer-featureCount
		setCompilerHints(totalDistinctFeatureCountMapper, 1, 1, intSize, -1);
		
		ReduceContract<PactNull, PactInteger, PactNull, PactInteger> totalDistinctFeatureCountReducer = 
			new ReduceContract<PactNull, PactInteger, PactNull, PactInteger>(TotalDistinctFeatureCountReducer.class, "Total distinct feature count (R)");
		totalDistinctFeatureCountReducer.setDegreeOfParallelism(1);
		totalDistinctFeatureCountReducer.setInput(totalDistinctFeatureCountMapper);
		setCompilerHints(totalDistinctFeatureCountReducer, 1, 1, intSize, 1);

		CrossContract<PactNull, PactInteger, PactNull, PactDouble, PactNull, ThetaNormalizerFactors> thetaFactorsSigmaVocab = 
			new CrossContract<PactNull, PactInteger, PactNull, PactDouble, PactNull, ThetaNormalizerFactors>(BayesThetaNormalizer.ThetaFactorsVocabCountSigmaJSigmaK.class, "Theta Factors Combiner 1");
		thetaFactorsSigmaVocab.setDegreeOfParallelism(1);
		thetaFactorsSigmaVocab.setFirstInput(totalDistinctFeatureCountReducer);
		thetaFactorsSigmaVocab.setSecondInput(totalSummerReducer);
		setCompilerHints(thetaFactorsSigmaVocab, 1, 1, doubleSize+doubleSize+intSize, 1);
		
		CrossContract<PactNull, ThetaNormalizerFactors, Label, PactDouble, Label, ThetaNormalizerFactors> thetaFactorsLabelWeights = 
			new CrossContract<PactNull, ThetaNormalizerFactors, Label, PactDouble, Label, ThetaNormalizerFactors>(BayesThetaNormalizer.ThetaFactorsLabelWeights.class, "Theta Factors Combiner 2");
		thetaFactorsLabelWeights.setDegreeOfParallelism(1);
		thetaFactorsLabelWeights.setFirstInput(thetaFactorsSigmaVocab);
		thetaFactorsLabelWeights.setSecondInput(labelSummerReducer);
		setCompilerHints(thetaFactorsLabelWeights, numLabels, 1, doubleSize+doubleSize+intSize+labelSize, 1);
		
		CoGroupContract<Label, PactDouble, ThetaNormalizerFactors, Label, PactDouble> thetaNormalizedLabels =
			new CoGroupContract<Label, PactDouble, ThetaNormalizerFactors, Label, PactDouble>(BayesThetaNormalizer.ThetaNormalize.class, "Theta Normalizer");
		thetaNormalizedLabels.setDegreeOfParallelism(noSubTasks);
		thetaNormalizedLabels.setFirstInput(tfidfTransformMapper);
		thetaNormalizedLabels.setSecondInput(thetaFactorsLabelWeights);
		setCompilerHints(thetaNormalizedLabels, numLabels, 1, labelSize+doubleSize, 1);		
		
		
		DataSinkContract<LabelFeaturePair, PactDouble> idfSink = 
			new DataSinkContract<LabelFeaturePair, PactDouble>(IdfOutputFormat.class, idfOutputPath, "IDF Sink");
		idfSink.setInput(tfIdfCalculatorMatcher);
		idfSink.setDegreeOfParallelism(noSubTasks);
		
		DataSinkContract<Label, PactDouble> thetaNormalizedSink = 
			new DataSinkContract<Label, PactDouble>(ThetaNormalizedOutputFormat.class, thetaNormalizerOutputPath, "Theta Normalizer Sink");
		thetaNormalizedSink.setInput(thetaNormalizedLabels);
		thetaNormalizedSink.setDegreeOfParallelism(noSubTasks);
		
		DataSinkContract<PactNull, PactDouble> sigmaKSigmaJSink = 
			new DataSinkContract<PactNull, PactDouble>(TotalSumOutputFormat.class, sigmaKSigmaJOutputPath, "Total Summer Sink");
		sigmaKSigmaJSink.setInput(totalSummerReducer);
		sigmaKSigmaJSink.setDegreeOfParallelism(1);
		
		DataSinkContract<Feature, PactDouble> sigmaJSink = 
			new DataSinkContract<Feature, PactDouble>(FeatureSumOutputFormat.class, sigmaJOutputPath, "Feature Summer Sink");
		sigmaJSink.setInput(featureSummerReducer);
		sigmaJSink.setDegreeOfParallelism(noSubTasks);
		
		DataSinkContract<Label, PactDouble> sigmaKSink = 
			new DataSinkContract<Label, PactDouble>(LabelSumOutputFormat.class, sigmaKOutputPath, "Label Summer Sink");
		sigmaKSink.setInput(labelSummerReducer);
		sigmaKSink.setDegreeOfParallelism(1);
		
		
		Collection<DataSinkContract<?, ?>> sinks = new LinkedList<DataSinkContract<?,?>>();
		sinks.add(idfSink);
		sinks.add(thetaNormalizedSink);
		sinks.add(sigmaKSink);
		sinks.add(sigmaJSink);
		sinks.add(sigmaKSigmaJSink);
		
		return new Plan(sinks);
	}
	
	private void setCompilerHints(Contract c, long keyCardinality, float recordsPerStubCall,
			float bytesPerRecord, float valuesPerKey) {
		if(keyCardinality > 0) {
			c.getCompilerHints().setKeyCardinality(keyCardinality);
		}
		if(recordsPerStubCall >= 0) {
			c.getCompilerHints().setAvgRecordsEmittedPerStubCall(recordsPerStubCall);
		}
		if(bytesPerRecord >= 0) {
			c.getCompilerHints().setAvgBytesPerRecord(bytesPerRecord);
		}
		if(valuesPerKey >= 0) {
			c.getCompilerHints().setAvgNumValuesPerKey(valuesPerKey);
		}
		
	}

}
