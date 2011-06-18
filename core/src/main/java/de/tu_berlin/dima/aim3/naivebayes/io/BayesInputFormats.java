package de.tu_berlin.dima.aim3.naivebayes.io;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesInputFormats {

	public static class WeightInputFormat extends BinaryInputFormat<PactString, PactDouble> {
		public WeightInputFormat(String path) throws IOException,URISyntaxException {
			super(path);
		}
	}
	
	public static class IdfInputFormat extends BinaryInputFormat<LabelFeaturePair, PactDouble> {
		public IdfInputFormat(String path) throws IOException, URISyntaxException {
			super(path);
		}
	}
	
	public static class ThetaNormalizedInputFormat extends BinaryInputFormat<PactString, PactDouble> {
		public ThetaNormalizedInputFormat(String path) throws IOException, URISyntaxException {
			super(path);
		}
	}
	
	
	public static class NaiveBayesDataInputFormat extends TextInputFormat<PactString, FeatureList> {

		@Override
		public boolean readLine(KeyValuePair<PactString, FeatureList> pair, byte[] line) {
			String lineString = new String(line);
			StringTokenizer tokenizer = new StringTokenizer(lineString, "\t");
			if (tokenizer.hasMoreTokens())
			{
				pair.setKey(new PactString(tokenizer.nextToken()));

				FeatureList value = new FeatureList();
				while (tokenizer.hasMoreTokens())
				{
					value.add(new PactString(tokenizer.nextToken(" ")));
				}
				pair.setValue(value);
				return true;
			}
			return false;
		}

	}
	
}
