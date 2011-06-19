package de.tu_berlin.dima.aim3.naivebayes.io;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactNull;

public class BayesInputFormats {

	public static class LabelSumInputFormat extends BinaryInputFormat<Label, PactDouble> {
		public LabelSumInputFormat(String path) throws IOException,URISyntaxException {
			super(path);
		}
	}
	
	public static class FeatureSumInputFormat extends BinaryInputFormat<Feature, PactDouble> {
		public FeatureSumInputFormat(String path) throws IOException,URISyntaxException {
			super(path);
		}
	}
	
	public static class TotalSumInputFormat extends BinaryInputFormat<PactNull, PactDouble> {
		public TotalSumInputFormat(String path) throws IOException,URISyntaxException {
			super(path);
		}
	}
	
	public static class IdfInputFormat extends BinaryInputFormat<LabelFeaturePair, PactDouble> {
		public IdfInputFormat(String path) throws IOException, URISyntaxException {
			super(path);
		}
	}
	
	public static class ThetaNormalizedInputFormat extends BinaryInputFormat<Label, PactDouble> {
		public ThetaNormalizedInputFormat(String path) throws IOException, URISyntaxException {
			super(path);
		}
	}
	
	
	public static class NaiveBayesDataInputFormat extends TextInputFormat<Label, FeatureList> {

		@Override
		public boolean readLine(KeyValuePair<Label, FeatureList> pair, byte[] line) {
			String lineString = new String(line);
			StringTokenizer tokenizer = new StringTokenizer(lineString, "\t");
			if (tokenizer.hasMoreTokens())
			{
				pair.setKey(new Label(tokenizer.nextToken().getBytes()));

				FeatureList value = new FeatureList();
				if (tokenizer.hasMoreTokens())
				{
					String featureList = tokenizer.nextToken();
					StringTokenizer featureTokenizer = new StringTokenizer(featureList, " ");
					while (featureTokenizer.hasMoreTokens())
					{
						value.add(new Feature(featureTokenizer.nextToken().getBytes()));	
					}
				}
				pair.setValue(value);
				return true;
			}
			return false;
		}

	}
	
}
