package de.tu_berlin.dima.aim3.naivebayes;

import java.util.StringTokenizer;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactString;

public class NaiveBayesInputFormat extends TextInputFormat<PactString, FeatureList> {

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
