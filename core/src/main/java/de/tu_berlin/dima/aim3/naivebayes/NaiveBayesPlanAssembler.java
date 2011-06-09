package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesPlanAssembler implements PlanAssembler,
		PlanAssemblerDescription {
	
	public static class IdOutFormat extends TextOutputFormat<PactString, PactInteger>
	{

		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			return (pair.getKey().toString() + ":\t" + pair.getValue().toString() + "\n").getBytes();
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
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String outputIds    = (args.length > 2 ? args[2] : "");
		String outputData    = (args.length > 3 ? args[3] : "");
		
		DataSourceContract<PactString, FeatureList> source = new DataSourceContract<PactString, FeatureList>
			(NaiveBayesInputFormat.class, dataInput, "Naive Bayes Input");
		source.setDegreeOfParallelism(noSubTasks);
		
		MapContract<PactString, FeatureList, PactInteger, PactString> uniqueIdentifierMapper = 
			new MapContract<PactString, FeatureList, PactInteger, PactString>
			(UniqueIdentifierMapper.class, "Unique Identifier Mapper");
		uniqueIdentifierMapper.setDegreeOfParallelism(noSubTasks);
		
		ReduceContract<PactInteger, PactString, PactString, PactInteger> uniqueIdentifierReducer = 
			new ReduceContract<PactInteger, PactString, PactString, PactInteger>
			(UniqueIdentifierReducer.class, "Unique Identifier Reducer");
		uniqueIdentifierReducer.setDegreeOfParallelism(1);
		
		DataSinkContract<PactString, PactInteger> dataSinkIds = 
			new DataSinkContract<PactString, PactInteger>(IdOutFormat.class, outputIds, "Output IDs");
		dataSinkIds.setDegreeOfParallelism(1);
		
		
		
		ReduceContract<PactString, FeatureList, PactString, FeatureList> labelAggregator = 
			new ReduceContract<PactString, FeatureList, PactString, FeatureList>(LabelAggregator.class, "Label Aggregator");
		labelAggregator.setDegreeOfParallelism(noSubTasks);
		
		DataSinkContract<PactString, FeatureList> dataSinkData = 
			new DataSinkContract<PactString, FeatureList>(FeatureListOutFormat.class, outputData, "Output Features");
		dataSinkData.setDegreeOfParallelism(noSubTasks);
		
		uniqueIdentifierMapper.setInput(source);
		
		uniqueIdentifierReducer.setInput(uniqueIdentifierMapper);
		dataSinkIds.setInput(uniqueIdentifierReducer);
		
		labelAggregator.setInput(source);
		dataSinkData.setInput(labelAggregator);
		
		
		
		MapContract<PactString, PactInteger, PactInteger, PactInteger> overallWordCountMapper =
			new MapContract<PactString, PactInteger, PactInteger, PactInteger>(OverallWordCountMapper.class, "Overall word count mapper");
		overallWordCountMapper.setDegreeOfParallelism(noSubTasks);
		overallWordCountMapper.setInput(null); //trainer-featureCount
		
		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> overallWordCountReducer = 
			new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(OverallWordcountReducer.class, "Overall word count reducer");
		overallWordCountReducer.setDegreeOfParallelism(noSubTasks);
		overallWordCountReducer.setInput(overallWordCountMapper);
		//output of overallWordCountReducer is trainer-vocabCount
		
		MatchContract<PactString, PactDouble, PactPair<PactString, PactDouble>, PactPair<PactString,PactString>, PactDouble> weightCalculatorMatcher =
			new MatchContract<PactString, PactDouble, PactPair<PactString,PactDouble>, PactPair<PactString,PactString>, PactDouble>(WeightCalculator.class, "Weight Calculator Matcher");
		weightCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		weightCalculatorMatcher.setFirstInput(null); //trainerDocCount
		weightCalculatorMatcher.setSecondInput(null); //documentFrequency (trainer-termDocCount)
		
		MatchContract<PactPair<PactString, PactString>, PactDouble, PactDouble, PactPair<PactString, PactString>, PactDouble> idfCalculatorMatcher = 
			new MatchContract<PactPair<PactString,PactString>, PactDouble, PactDouble, PactPair<PactString,PactString>, PactDouble>(IdfCalculator.class, "Idf Calculator Matcher");
		idfCalculatorMatcher.setDegreeOfParallelism(noSubTasks);
		idfCalculatorMatcher.setFirstInput(weightCalculatorMatcher);
		idfCalculatorMatcher.setSecondInput(null); //weight (trainer-wordFreq)
		//output of idfCalculator is trainer-tfIdf
		
		
		Collection<DataSinkContract<?,?>> dataSinks = new LinkedList<DataSinkContract<?,?>>();
		dataSinks.add(dataSinkIds);
		dataSinks.add(dataSinkData);
		Plan plan = new Plan(dataSinks);
		return plan;
	}

}
