/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tu_berlin.dima.aim3.naivebayes.io;

import java.io.IOException;
import java.net.URISyntaxException;

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
			FeatureList value = new FeatureList();
			
			//Read label
			int pos = -1;
			int start = 0;
			int len = 0;
			while(++pos < line.length && line[pos] != '\t');
			//If no label was found exit
			if(pos == line.length) {
				return false;
			}
			
			len = pos - start;
			byte[] labelArr = new byte[len];
			System.arraycopy(line, start, labelArr, 0, len);
			start = pos + 1;
			pair.setKey(new Label(labelArr));
			
			//If no features exit
			if(start == line.length) {
				return false;
			}
			
			//Read features
			while(true) {
				while(++pos < line.length && line[pos] != ' ');
				len = pos - start;
				byte[] featureArr = new byte[len];
				System.arraycopy(line, start, featureArr, 0, len);
				value.add(new Feature(featureArr));
				
				//Skip to next non space character
				while(++pos < line.length && line[pos] == ' ');
				start = pos;
				if(pos >= line.length) {
					break;
				}
			}
			pair.setValue(value);
			
			return true;
		}
	}
}
