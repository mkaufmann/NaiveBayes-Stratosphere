package de.tu_berlin.dima.aim3.naivebayes.classifier;

import de.tu_berlin.dima.aim3.naivebayes.NaiveBayesInputFormat;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelPair;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class NBayesClassifierPlanAssembler implements PlanAssembler {

	public static class LabelPairIntegerOutputFormat extends TextOutputFormat<LabelPair, PactInteger> {
		@Override
		public byte[] writeLine(KeyValuePair<LabelPair, PactInteger> keyValue) {
			String result = keyValue.getKey().getFirst() + "-" + keyValue.getKey().getSecond()
			+ " :: " + keyValue.getValue().getValue() + "\n";
			return result.getBytes();
		}
	}
	
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		if(args == null) args = new String[0];
		
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : -1);
		String testData = (args.length > 1 ? args[1] : "file:///home/mkaufmann/datasets/test");
		String modelData = (args.length > 2 ? args[2] : "file:///home/mkaufmann/datasets/model");
		String outputData    = (args.length > 3 ? args[3] : "file:///home/mkaufmann/datasets/result");
		
		DataSourceContract<PactString, FeatureList> source = 
			new DataSourceContract<PactString, FeatureList>(NaiveBayesInputFormat.class, testData, "Classifier test data");
		source.setDegreeOfParallelism(noSubTasks);
		
		MapContract<PactString, FeatureList, LabelPair, PactInteger> classifier =
			new MapContract<PactString, FeatureList, LabelPair, PactInteger>(ClassifyingMapper.class, "Classifying mapper");
		classifier.setDegreeOfParallelism(noSubTasks);
		classifier.setInput(source);
		classifier.setStubParameter(ClassifyingMapper.MODEL_BASE_PATH, modelData);
		
		ReduceContract<LabelPair, PactInteger, LabelPair, PactInteger> summer =
			new ReduceContract<LabelPair, PactInteger, LabelPair, PactInteger>(ClassifyingReducer.class, "Summer");
		summer.setDegreeOfParallelism(noSubTasks);
		summer.setInput(classifier);
		
		DataSinkContract<LabelPair, PactInteger> sink =
			new DataSinkContract<LabelPair, PactInteger>(LabelPairIntegerOutputFormat.class, outputData, "Classifier results");
		sink.setDegreeOfParallelism(noSubTasks);
		sink.setInput(summer);
		
		return new Plan(sink);
	}

}
