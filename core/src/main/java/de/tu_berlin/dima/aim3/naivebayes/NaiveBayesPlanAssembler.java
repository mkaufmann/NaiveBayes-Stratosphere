package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import de.tu_berlin.dima.aim3.naivebayes.data.NormalizedTokenCountList;
import de.tu_berlin.dima.aim3.naivebayes.data.TokenCountPair;
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
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesPlanAssembler implements PlanAssembler{
	
	public static class LabelTokenDoubleOutFormat extends TextOutputFormat<LabelTokenPair, PactInteger> {
		@Override
		public byte[] writeLine(KeyValuePair<LabelTokenPair, PactInteger> pair) {
			String str = pair.getKey().getFirst() + "/" + pair.getKey().getSecond() + " :: " + pair.getValue().getValue() + "\r\n";
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
		
		DataSourceContract<PactString, FeatureList> source = new DataSourceContract<PactString, FeatureList>
			(NaiveBayesInputFormat.class, dataInput, "Naive Bayes Input");
		source.setDegreeOfParallelism(noSubTasks);
		
		MapContract<PactString, FeatureList, PactString, NormalizedTokenCountList> featureBaseMapper = 
			new MapContract<PactString, FeatureList, PactString, NormalizedTokenCountList>(BayesFeatureMapper.Base.class);
		featureBaseMapper.setDegreeOfParallelism(noSubTasks);
		featureBaseMapper.setInput(source);
		
		MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble> featureCountMapper = 
			new MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble>(BayesFeatureMapper.FeatureCount.class);
		featureCountMapper.setDegreeOfParallelism(noSubTasks);
		featureCountMapper.setInput(featureBaseMapper);
		
		ReduceContract<PactString, PactDouble, PactString, PactDouble> featureCountReducer = 
			new ReduceContract<PactString, PactDouble, PactString, PactDouble>(BayesFeatureReducer.FeatureCount.class);
		featureCountReducer.setDegreeOfParallelism(noSubTasks);
		featureCountReducer.setInput(featureCountMapper);
		
		MapContract<PactString, NormalizedTokenCountList, PactString, PactInteger> labelCountMapper = 
			new MapContract<PactString, NormalizedTokenCountList, PactString, PactInteger>(BayesFeatureMapper.LabelCount.class);
		labelCountMapper.setDegreeOfParallelism(noSubTasks);
		labelCountMapper.setInput(featureBaseMapper);
		
		ReduceContract<PactString, PactInteger, PactString, PactInteger> labelCountReducer = 
			new ReduceContract<PactString, PactInteger, PactString, PactInteger>(BayesFeatureReducer.LabelCount.class);
		labelCountReducer.setDegreeOfParallelism(noSubTasks);
		labelCountReducer.setInput(labelCountMapper);
		
		MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble> featureTfMapper = 
			new MapContract<PactString, NormalizedTokenCountList, PactString, PactDouble>(BayesFeatureMapper.FeatureTf.class);
		featureTfMapper.setDegreeOfParallelism(noSubTasks);
		featureTfMapper.setInput(featureBaseMapper);
		
		ReduceContract<PactString, PactDouble, PactString, PactDouble> featureTfReducer = 
			new ReduceContract<PactString, PactDouble, PactString, PactDouble>(BayesFeatureReducer.FeatureTf.class);
		featureTfReducer.setDegreeOfParallelism(noSubTasks);
		featureTfReducer.setInput(featureTfMapper);
		
		MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactInteger> dfMapper = 
			new MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactInteger>(BayesFeatureMapper.DocumentFrequency.class);
		dfMapper.setDegreeOfParallelism(noSubTasks);
		dfMapper.setInput(featureBaseMapper);
		
		ReduceContract<LabelTokenPair, PactInteger, LabelTokenPair, PactInteger> dfReducer = 
			new ReduceContract<LabelTokenPair, PactInteger, LabelTokenPair, PactInteger>(BayesFeatureReducer.DocumentFrequency.class);
		dfReducer.setDegreeOfParallelism(noSubTasks);
		dfReducer.setInput(dfMapper);
		
		MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactDouble> weightMapper = 
			new MapContract<PactString, NormalizedTokenCountList, LabelTokenPair, PactDouble>(BayesFeatureMapper.Weight.class);
		weightMapper.setDegreeOfParallelism(noSubTasks);
		weightMapper.setInput(featureBaseMapper);
		
		ReduceContract<LabelTokenPair, PactDouble, LabelTokenPair, PactDouble> weightReducer = 
			new ReduceContract<LabelTokenPair, PactDouble, LabelTokenPair, PactDouble>(BayesFeatureReducer.Weight.class);
		weightReducer.setDegreeOfParallelism(noSubTasks);
		weightReducer.setInput(weightMapper);
		
		DataSinkContract<LabelTokenPair, PactInteger> sink = 
			new DataSinkContract<LabelTokenPair, PactInteger>(LabelTokenDoubleOutFormat.class, dataOutput);
		sink.setInput(dfReducer);
		
		
		MapContract<PactString, PactInteger, PactInteger, PactInteger> overallWordCountMapper =
			new MapContract<PactString, PactInteger, PactInteger, PactInteger>(OverallWordCountMapper.class, "Overall word count mapper");
		overallWordCountMapper.setDegreeOfParallelism(noSubTasks);
		overallWordCountMapper.setInput(null); //trainer-featureCount
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> overallWordCountReducer = 
			new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(OverallWordcountReducer.class, "Overall word count reducer");
		overallWordCountReducer.setDegreeOfParallelism(noSubTasks);
		overallWordCountReducer.setInput(overallWordCountMapper);
		//output of overallWordCountReducer is trainer-vocabCount
		
		MatchContract<PactString, PactDouble, TokenCountPair, LabelTokenPair, PactDouble> weightCalculatorMatcher =
			new MatchContract<PactString, PactDouble, TokenCountPair, LabelTokenPair, PactDouble>(WeightCalculator.class, "Weight Calculator Matcher");
		weightCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		weightCalculatorMatcher.setFirstInput(null); //trainerDocCount
		weightCalculatorMatcher.setSecondInput(null); //documentFrequency (trainer-termDocCount)
		
		MatchContract<LabelTokenPair, PactDouble, PactDouble, LabelTokenPair, PactDouble> idfCalculatorMatcher = 
			new MatchContract<LabelTokenPair, PactDouble, PactDouble, LabelTokenPair, PactDouble>(IdfCalculator.class, "Idf Calculator Matcher");
		idfCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		idfCalculatorMatcher.setFirstInput(weightCalculatorMatcher);
		idfCalculatorMatcher.setSecondInput(null); //weight (trainer-wordFreq)
		//output of idfCalculator is trainer-tfIdf
		
		return new Plan(sink);
	}

}
