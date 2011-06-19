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

package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Map.Entry;

import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.data.TfList;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class BayesFeatureMapper  {
	private static final PactInteger INT_ONE = new PactInteger(1);
	
	private static int gramSize = 1;
	
	/**
	 * Counts the length normalized and tf transformed, term frequency per feature per document
	 * @author mkaufmann
	 *
	 */
	public static class Base extends MapStub<Label, FeatureList, Label, TfList> {
		@Override
		public void map(final Label label, final FeatureList features,
				final Collector<Label, TfList> out) {
			TfList featureList = new TfList();
			
			//Count # of times a feature occurs in document
			if (gramSize > 1) {
				//TODO:!!!!!
			} else {
				for (Feature token : features) {
					if (featureList.containsKey(token)) {
						featureList.put(token, 1 + featureList.get(token));
					} else {
						featureList.put(token, 1);
					}
				}
			}
			
			out.collect(label, featureList);
		}
	}
	
	// Output Document Frequency per Feature per Class
	public static class DocumentFrequency extends MapStub<Label, TfList, LabelFeaturePair, PactInteger> {		
		@Override
		public void map(Label label, TfList featureList,
				Collector<LabelFeaturePair, PactInteger> out) {
			for (Entry<Feature, Integer> entry : featureList.entrySet()) {
				Feature feature = entry.getKey();
				
				LabelFeaturePair labelFeature = new LabelFeaturePair();
		        labelFeature.setFirst(label);
		        labelFeature.setSecond(feature);
		        out.collect(labelFeature, INT_ONE);
			}
		}
	}
	
	/**
	 * Count # of occurrences per feature in all documents
	 * 
	 * Maps each feature to one
	 */
	public static class DistinctFeatureCount extends MapStub<Label, TfList, Feature, PactInteger> {		
		@Override
		public void map(Label label, TfList featureList,
				Collector<Feature, PactInteger> out) {
			for (Entry<Feature, Integer> entry : featureList.entrySet()) {
				Feature feature = entry.getKey();
		        out.collect(feature, INT_ONE);
			}
		}

	}
	
	// Corpus Term Frequency (FEATURE_TF)
	/**
	 * # of occurences per feature
	 */
	public static class FeatureTf extends MapStub<Label, TfList, Feature, PactInteger> {
		@Override
		public void map(Label label, TfList featureList,
				Collector<Feature, PactInteger> out) {
			for (Entry<Feature, Integer> entry : featureList.entrySet()) {
				Feature feature = entry.getKey();
				int tf = entry.getValue();
				
				out.collect(feature, new PactInteger(tf));
			}
		}

	}

	/**
	 * Count # of occurrences per label in all documents.
	 * 
	 * Maps each label to 1. 
	 * @author mkaufmann
	 *
	 */
	public static class LabelCount extends MapStub<Label, TfList, Label, PactInteger> {
		@Override
		public void map(Label label, TfList featureList,
				Collector<Label, PactInteger> out) {
		    out.collect(label, INT_ONE);
		}

	}
	
    // Output Length Normalized + TF Transformed Frequency per Word per Class
    // Log(1 + D_ij)/SQRT( SIGMA(k, D_kj) )
	public static class NormalizedTf extends MapStub<Label, TfList, LabelFeaturePair, PactDouble> {
		@Override
		public void map(Label label, TfList featureList,
				Collector<LabelFeaturePair, PactDouble> out) {
			// factor = sqrt((sum of all feature counts in documents)^2)
			double lengthNormalisationFactor = 0;
			for (Entry<Feature, Integer> entry : featureList.entrySet()) {
				int featureCount = entry.getValue();
				lengthNormalisationFactor += featureCount * featureCount;
			}
			lengthNormalisationFactor = Math.sqrt(lengthNormalisationFactor);
			
			//Normalize feature frequency
			for (Entry<Feature, Integer> entry : featureList.entrySet()) {
				Feature feature = entry.getKey();
				int tf = entry.getValue();
				
				LabelFeaturePair tuple = new LabelFeaturePair();
				tuple.setFirst(label);
				tuple.setSecond(feature);
				
				PactDouble normalizedTf = new PactDouble(Math.log(1.0 + tf) / lengthNormalisationFactor);
				out.collect(tuple, normalizedTf);
			}
		}

	}

}
