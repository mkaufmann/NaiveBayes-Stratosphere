package de.tu_berlin.dima.aim3.naivebayes.classifier;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.classifier.ConfusionMatrix;

import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelPair;
import de.tu_berlin.dima.aim3.naivebayes.io.BayesInputFormats.NaiveBayesDataInputFormat;
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
			String result = keyValue.getKey().getFirst() + "::" + keyValue.getKey().getSecond()
			+ "::" + keyValue.getValue().getValue() + "\n";
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
			new DataSourceContract<PactString, FeatureList>(NaiveBayesDataInputFormat.class, testData, "Classifier test data");
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

	public static void main(String... args) throws NumberFormatException, IOException {
		if(args == null) args = new String[0];
		
		String resultData    = (args.length > 0 ? args[0] : "file:///home/mkaufmann/datasets/result");
		
		Map<String, Map<String, Integer>> confusionMatrix = 
			new HashMap<String, Map<String, Integer>>();
		
		InputStreamReader stream = new InputStreamReader(new FileInputStream(new File(resultData)));
		BufferedReader in = new BufferedReader(stream);
		
		String line = null;
		while((line =in.readLine()) != null) {
			String[] triple = line.split("::");
			if(triple.length == 3) {
				String correct = triple[0];
				String label = triple[1];
				int count = Integer.parseInt(triple[2]);
				
				Map<String, Integer> rowMatrix = confusionMatrix.get(correct);
				if (rowMatrix == null) {
					rowMatrix = new HashMap<String, Integer>();
				}
				
				rowMatrix.put(label, count);
				confusionMatrix.put(correct, rowMatrix);
			}
		}

		ConfusionMatrix matrix = new ConfusionMatrix(confusionMatrix.keySet(),
				"default");
		for (Map.Entry<String, Map<String, Integer>> correctLabelSet : confusionMatrix
				.entrySet()) {
			Map<String, Integer> rowMatrix = correctLabelSet.getValue();
			for (Map.Entry<String, Integer> classifiedLabelSet : rowMatrix
					.entrySet()) {
				matrix.addInstance(correctLabelSet.getKey(), classifiedLabelSet
						.getKey());
				matrix.putCount(correctLabelSet.getKey(), classifiedLabelSet
						.getKey(), classifiedLabelSet.getValue());
			}
		}
		
		System.out.println(matrix.toString());
	}
}
