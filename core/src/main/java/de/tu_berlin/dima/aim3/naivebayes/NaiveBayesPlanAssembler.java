package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.IdfCalculator;
import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.OverallWordCountMapper;
import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.OverallWordcountReducer;
import de.tu_berlin.dima.aim3.naivebayes.BayesTfIdf.WeightCalculator;
import de.tu_berlin.dima.aim3.naivebayes.classifier.PactBayesDatastore;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import de.tu_berlin.dima.aim3.naivebayes.data.NormalizedTokenCountList;
import de.tu_berlin.dima.aim3.naivebayes.data.ThetaNormalizerFactors;
import de.tu_berlin.dima.aim3.naivebayes.data.TokenCountPair;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.NaiveBayesDataInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.IdfOutputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.ThetaNormalizedOutputFormat;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesOutputFormats.WeightOutputFormat;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesPlanAssembler implements PlanAssembler{
	
	public static class LabelTokenDoubleOutFormat extends TextOutputFormat<LabelTokenPair, PactDouble> {
		@Override
		public byte[] writeLine(KeyValuePair<LabelTokenPair, PactDouble> pair) {
			String str = pair.getKey().getFirst() + "/" + pair.getKey().getSecond() + " :: " + pair.getValue().getValue() + "\r\n";
			return str.getBytes();
		}
	}
	
	public static class StringTokenCountPairOutFormat extends TextOutputFormat<PactString, TokenCountPair> {
		@Override
		public byte[] writeLine(KeyValuePair<PactString, TokenCountPair> pair) {
			String str = pair.getKey().getValue() + " :: " + pair.getValue().getFirst() + " : " + pair.getValue().getSecond() + "\r\n";
			return str.getBytes();
		}
	}
	
	public static class StringDoubleOutFormat extends TextOutputFormat<PactString, PactDouble> {
		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactDouble> pair) {
			String str = pair.getKey().getValue() + " :: " + pair.getValue().getValue() + "\r\n";
			return str.getBytes();
		}
	}
	
	public static class StringIntegerOutFormat extends TextOutputFormat<PactString, PactInteger> {
		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			String str = pair.getKey().getValue() + " :: " + pair.getValue().getValue() + "\r\n";
			return str.getBytes();
		}
	}
	
	public static class FeatureListOutFormat extends TextOutputFormat<PactString, FeatureList>
	{

		@Override
		public byte[] writeLine(KeyValuePair<PactString, FeatureList> pair) {
			StringBuilder builder = new StringBuilder(pair.getKey().toString() + ":\t");
			Iterator<PactString> it = pair.getValue().iterator();
			while (it.hasNext())
			{
				builder.append(it.next() + " ");
			}
			builder.append('\n');
			return builder.toString().getBytes();
		}
		
	}

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
		
		DataSourceContract<PactString, FeatureList> source = new DataSourceContract<PactString, FeatureList>
			(NaiveBayesDataInputFormat.class, dataInput, "Naive Bayes Input");
		source.setDegreeOfParallelism(noSubTasks);
		source.getCompilerHints().setAvgBytesPerRecord(1500);
		
		MapContract<PactString, FeatureList, PactString, NormalizedTokenCountList> featureBaseMapper = 
			new MapContract<PactString, FeatureList, PactString, NormalizedTokenCountList>(BayesFeatureMapper.Base.class, "Feature/Token generator");
		featureBaseMapper.setDegreeOfParallelism(noSubTasks);
		featureBaseMapper.setInput(source);
		featureBaseMapper.getCompilerHints().setKeyCardinality(numLabels);
		setCompilerHints(featureBaseMapper, numLabels, 1, labelSize + avgFeaturesPerDocument*(featureSize+intSize), -1);
		
		MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble> featureCountMapper = 
			new MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble>(BayesFeatureMapper.FeatureCount.class, "Feature Count Mapper");
		featureCountMapper.setDegreeOfParallelism(noSubTasks);
		featureCountMapper.setInput(featureBaseMapper);
		setCompilerHints(featureCountMapper, numFeatures, avgFeaturesPerDocument, featureSize+doubleSize, -1);
		
		ReduceContract<PactString, PactDouble, PactString, PactDouble> featureCountReducer = 
			new ReduceContract<PactString, PactDouble, PactString, PactDouble>(BayesFeatureReducer.FeatureCount.class, "Feature Count Reducer");
		featureCountReducer.setDegreeOfParallelism(noSubTasks);
		featureCountReducer.setInput(featureCountMapper);
		setCompilerHints(featureCountReducer, numFeatures, 1, labelSize+intSize, 1);
		
		MapContract<PactString, NormalizedTokenCountList, PactString, PactInteger> labelCountMapper = 
			new MapContract<PactString, NormalizedTokenCountList, PactString, PactInteger>(BayesFeatureMapper.LabelCount.class, "Label Count Mapper");
		labelCountMapper.setDegreeOfParallelism(noSubTasks);
		labelCountMapper.setInput(featureBaseMapper);
		setCompilerHints(labelCountMapper, numLabels, 1, labelSize+intSize, 1);
		
		ReduceContract<PactString, PactInteger, PactString, PactInteger> labelCountReducer = 
			new ReduceContract<PactString, PactInteger, PactString, PactInteger>(BayesFeatureReducer.LabelCount.class, "Label Count Reducer");
		labelCountReducer.setDegreeOfParallelism(noSubTasks);
		labelCountReducer.setInput(labelCountMapper);
		setCompilerHints(labelCountReducer, numLabels, 1, labelSize+intSize, 1);
		
		MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble> featureTfMapper = 
			new MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble>(BayesFeatureMapper.FeatureTf.class, "Feature Frequency Mapper");
		featureTfMapper.setDegreeOfParallelism(noSubTasks);
		featureTfMapper.setInput(featureBaseMapper);
		setCompilerHints(featureTfMapper, numFeatures, avgFeaturesPerDocument, featureSize+doubleSize, -1);
		
		ReduceContract<PactString, PactDouble, PactString, PactDouble> featureTfReducer = 
			new ReduceContract<PactString, PactDouble, PactString, PactDouble>(BayesFeatureReducer.FeatureTf.class, "Feature Frequency Reducer");
		featureTfReducer.setDegreeOfParallelism(noSubTasks);
		featureTfReducer.setInput(featureTfMapper);
		setCompilerHints(featureTfReducer, numFeatures, 1, featureSize+doubleSize, 1);
		
		MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactInteger> dfMapper = 
			new MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactInteger>(BayesFeatureMapper.DocumentFrequency.class, "Document Frequency Mapper");
		dfMapper.setDegreeOfParallelism(noSubTasks);
		dfMapper.setInput(featureBaseMapper);
		setCompilerHints(dfMapper, numLabels*avgFeaturesPerLabel, avgFeaturesPerDocument, labelSize+featureSize+intSize, -1);
		
		ReduceContract<LabelTokenPair, PactInteger, PactString, TokenCountPair> dfReducer = 
			new ReduceContract<LabelTokenPair, PactInteger, PactString, TokenCountPair>(BayesFeatureReducer.DocumentFrequency.class, "Document Frequency Reducer");
		dfReducer.setDegreeOfParallelism(noSubTasks);
		dfReducer.setInput(dfMapper);
		dfReducer.getCompilerHints().setKeyCardinality(numFeatures);
		setCompilerHints(dfReducer, numLabels, 1, labelSize+featureSize+doubleSize, -1);
		
		MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactDouble> weightMapper = 
			new MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactDouble>(BayesFeatureMapper.Weight.class, "Weight Mapper");
		weightMapper.setDegreeOfParallelism(noSubTasks);
		weightMapper.setInput(featureBaseMapper);
		setCompilerHints(weightMapper, numLabels*avgFeaturesPerDocument, avgFeaturesPerDocument, labelSize+featureSize+doubleSize, -1);
		
		ReduceContract<LabelTokenPair, PactDouble, LabelTokenPair, PactDouble> weightReducer = 
			new ReduceContract<LabelTokenPair, PactDouble, LabelTokenPair, PactDouble>(BayesFeatureReducer.Weight.class, "Weight Reducer");
		weightReducer.setDegreeOfParallelism(noSubTasks);
		weightReducer.setInput(weightMapper);
		setCompilerHints(weightReducer, numLabels*avgFeaturesPerLabel, 1, labelSize+featureSize+doubleSize, 1);
		
		MapContract<PactString, PactDouble, PactNull, PactInteger> overallWordCountMapper =
			new MapContract<PactString, PactDouble, PactNull, PactInteger>(OverallWordCountMapper.class, "Overall word count mapper");
		overallWordCountMapper.setDegreeOfParallelism(noSubTasks);
		overallWordCountMapper.setInput(featureCountReducer); //trainer-featureCount
		
		ReduceContract<PactNull, PactInteger, PactNull, PactInteger> overallWordCountReducer = 
			new ReduceContract<PactNull, PactInteger, PactNull, PactInteger>(OverallWordcountReducer.class, "Overall word count reducer");
		overallWordCountReducer.setDegreeOfParallelism(noSubTasks);
		overallWordCountReducer.setInput(overallWordCountMapper);
		overallWordCountReducer.getCompilerHints().setKeyCardinality(1);
		//output of overallWordCountReducer is trainer-vocabCount
		
		MatchContract<PactString, PactInteger, TokenCountPair, LabelTokenPair, PactDouble> weightCalculatorMatcher =
			new MatchContract<PactString, PactInteger, TokenCountPair, LabelTokenPair, PactDouble>(WeightCalculator.class, "Weight Calculator Matcher");
		weightCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		weightCalculatorMatcher.setFirstInput(labelCountReducer); //trainerDocCount
		weightCalculatorMatcher.setSecondInput(dfReducer); //documentFrequency (trainer-termDocCount)
		setCompilerHints(weightCalculatorMatcher, numLabels*avgFeaturesPerLabel, 1, labelSize+featureSize+doubleSize, -1);
		
		MatchContract<LabelTokenPair, PactDouble, PactDouble, LabelTokenPair, PactDouble> idfCalculatorMatcher = 
			new MatchContract<LabelTokenPair, PactDouble, PactDouble, LabelTokenPair, PactDouble>(IdfCalculator.class, "Idf Calculator Matcher");
		idfCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		idfCalculatorMatcher.setFirstInput(weightCalculatorMatcher);
		idfCalculatorMatcher.setSecondInput(weightReducer); //weight (trainer-wordFreq)
		setCompilerHints(idfCalculatorMatcher, numLabels*avgFeaturesPerLabel, 1, labelSize+featureSize+doubleSize, 1);
		//output of idfCalculator is trainer-tfIdf
		
		MapContract<LabelTokenPair, PactDouble, PactString, PactDouble> featureSummerMapper = 
			new MapContract<LabelTokenPair, PactDouble, PactString, PactDouble>(BayesWeightMapper.FeatureSummer.class, "Feature Idf Summer Mapper");
		featureSummerMapper.setDegreeOfParallelism(noSubTasks);
		featureSummerMapper.setInput(idfCalculatorMatcher);
		setCompilerHints(featureSummerMapper, numLabels, 1, featureSize, -1);
		
		ReduceContract<PactString, PactDouble, PactString, PactDouble> featureSummerReducer = 
			new ReduceContract<PactString, PactDouble, PactString, PactDouble>(BayesWeightReducer.Summer.class, "Feature Idf Summer Reducer");
		featureSummerReducer.setDegreeOfParallelism(noSubTasks);
		featureSummerReducer.setInput(featureSummerMapper);
		setCompilerHints(featureSummerReducer, numFeatures, 1, featureSize+doubleSize, 1);
		
		MapContract<LabelTokenPair, PactDouble, PactString, PactDouble> labelSummerMapper = 
			new MapContract<LabelTokenPair, PactDouble, PactString, PactDouble>(BayesWeightMapper.LabelSummer.class, "Weight Idf Summer Mapper");
		labelSummerMapper.setDegreeOfParallelism(noSubTasks);
		labelSummerMapper.setInput(idfCalculatorMatcher);
		
		ReduceContract<PactString, PactDouble, PactString, PactDouble> labelSummerReducer = 
			new ReduceContract<PactString, PactDouble, PactString, PactDouble>(BayesWeightReducer.Summer.class, "Weight Idf Summer Reducer");
		labelSummerReducer.setDegreeOfParallelism(noSubTasks);
		labelSummerReducer.setInput(labelSummerMapper);
		
		MapContract<LabelTokenPair, PactDouble, PactString, PactDouble> totalSummerMapper = 
			new MapContract<LabelTokenPair, PactDouble, PactString, PactDouble>(BayesWeightMapper.TotalSummer.class, "Total Sum Mapper");
		totalSummerMapper.setDegreeOfParallelism(noSubTasks);
		totalSummerMapper.setInput(idfCalculatorMatcher);
		
		ReduceContract<PactString, PactDouble, PactString, PactDouble> totalSummerReducer = 
			new ReduceContract<PactString, PactDouble, PactString, PactDouble>(BayesWeightReducer.Summer.class, "Total Sum Reducer");
		totalSummerReducer.setDegreeOfParallelism(noSubTasks);
		totalSummerReducer.setInput(totalSummerMapper);
		
		MapContract<LabelTokenPair, PactDouble, PactString, PactDouble> tfidfTransformMapper = 
			new MapContract<LabelTokenPair, PactDouble, PactString, PactDouble>(BayesThetaNormalizer.TfIdfTransform.class, "Tfidf Label Extractor");
		tfidfTransformMapper.setDegreeOfParallelism(noSubTasks);
		tfidfTransformMapper.setInput(idfCalculatorMatcher);
		setCompilerHints(tfidfTransformMapper, numLabels, 1, labelSize+doubleSize, -1);

		CrossContract<PactNull, PactInteger, PactString, PactDouble, PactNull, ThetaNormalizerFactors> thetaFactorsSigmaVocab = 
			new CrossContract<PactNull, PactInteger, PactString, PactDouble, PactNull, ThetaNormalizerFactors>(BayesThetaNormalizer.ThetaFactorsVocabCountSigmaJSigmaK.class, "Theta Factors Combiner 1");
		thetaFactorsSigmaVocab.setDegreeOfParallelism(noSubTasks);
		thetaFactorsSigmaVocab.setFirstInput(overallWordCountReducer);
		thetaFactorsSigmaVocab.setSecondInput(totalSummerReducer);
		
		CrossContract<PactNull, ThetaNormalizerFactors, PactString, PactDouble, PactString, ThetaNormalizerFactors> thetaFactorsLabelWeights = 
			new CrossContract<PactNull, ThetaNormalizerFactors, PactString, PactDouble, PactString, ThetaNormalizerFactors>(BayesThetaNormalizer.ThetaFactorsLabelWeights.class, "Theta Factors Combiner 2");
		thetaFactorsLabelWeights.setDegreeOfParallelism(noSubTasks);
		thetaFactorsLabelWeights.setFirstInput(thetaFactorsSigmaVocab);
		thetaFactorsLabelWeights.setSecondInput(labelSummerReducer);
		
		CoGroupContract<PactString, PactDouble, ThetaNormalizerFactors, PactString, PactDouble> thetaNormalizedLabels =
			new CoGroupContract<PactString, PactDouble, ThetaNormalizerFactors, PactString, PactDouble>(BayesThetaNormalizer.ThetaNormalize.class, "Theta Normalizer");
		thetaNormalizedLabels.setDegreeOfParallelism(noSubTasks);
		thetaNormalizedLabels.setFirstInput(tfidfTransformMapper);
		thetaNormalizedLabels.setSecondInput(thetaFactorsLabelWeights);
		setCompilerHints(thetaNormalizedLabels, numLabels, 1, labelSize+doubleSize, 1);
		
		
		
		
		DataSinkContract<LabelTokenPair, PactDouble> idfSink = 
			new DataSinkContract<LabelTokenPair, PactDouble>(IdfOutputFormat.class, idfOutputPath, "IDF Sink");
		idfSink.setInput(idfCalculatorMatcher);
		idfSink.setDegreeOfParallelism(noSubTasks);
		
		DataSinkContract<PactString, PactDouble> thetaNormalizedSink = 
			new DataSinkContract<PactString, PactDouble>(ThetaNormalizedOutputFormat.class, thetaNormalizerOutputPath, "Theta Normalizer Sink");
		thetaNormalizedSink.setInput(thetaNormalizedLabels);
		thetaNormalizedSink.setDegreeOfParallelism(noSubTasks);
		
		DataSinkContract<PactString, PactDouble> sigmaKSigmaJSink = 
			new DataSinkContract<PactString, PactDouble>(WeightOutputFormat.class, sigmaKSigmaJOutputPath, "Total Summer Sink");
		sigmaKSigmaJSink.setInput(totalSummerReducer);
		sigmaKSigmaJSink.setDegreeOfParallelism(1);
		
		DataSinkContract<PactString, PactDouble> sigmaJSink = 
			new DataSinkContract<PactString, PactDouble>(WeightOutputFormat.class, sigmaJOutputPath, "Feature Summer Sink");
		sigmaJSink.setInput(featureSummerReducer);
		sigmaJSink.setDegreeOfParallelism(noSubTasks);
		
		DataSinkContract<PactString, PactDouble> sigmaKSink = 
			new DataSinkContract<PactString, PactDouble>(WeightOutputFormat.class, sigmaKOutputPath, "Label Summer Sink");
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
