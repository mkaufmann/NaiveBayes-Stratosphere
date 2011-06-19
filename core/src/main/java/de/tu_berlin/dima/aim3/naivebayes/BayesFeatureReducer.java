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

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureCountPair;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class BayesFeatureReducer {
	private static double minDf = -1;
	
	
	/**
	 * Count # of occurrences per label in all documents.
	 * 
	 * Sums up # of occurrences for a specific label.
	 *
	 */
	@SameKey
	@Combinable
	public static class LabelCount extends ReduceStub<Label, PactInteger, Label, PactInteger> {
		@Override
		public void reduce(Label label, Iterator<PactInteger> count,
				Collector<Label, PactInteger> out) {
			int sum = 0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			out.collect(label, new PactInteger(sum));
		}

		@Override
		public void combine(Label label, Iterator<PactInteger> counts,
				Collector<Label, PactInteger> out) {
			reduce(label, counts, out);
		}
	}
	
	@SameKey
	@Combinable
	public static class FeatureTf extends ReduceStub<Feature, PactInteger, Feature, PactInteger> {
		@Override
		public void reduce(Feature feature, Iterator<PactInteger> count,
				Collector<Feature, PactInteger> out) {
			int sum = 0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			out.collect(feature, new PactInteger(sum));
		}

		@Override
		public void combine(Feature feature, Iterator<PactInteger> counts,
				Collector<Feature, PactInteger> out) {
			reduce(feature, counts, out);
		}
	}
	
	/**
	 * Count # of occurrences per feature in all documents
	 * 
	 * Sum up the number of occurences for each word
	 * @author mkaufmann
	 *
	 */
	@SameKey
	@Combinable
	public static class FeatureCount extends ReduceStub<Feature, PactInteger, Feature, PactInteger> {
		@Override
		public void reduce(Feature feature, Iterator<PactInteger> count,
				Collector<Feature, PactInteger> out) {
			int currentCorpusDf = 0;
			while(count.hasNext()) {
				currentCorpusDf += count.next().getValue();
			}
			
			if (minDf > 0.0 && currentCorpusDf < minDf) {
				System.out.println("Skipped " + feature.toString() + " less than minDf");
			} else {
				out.collect(feature, new PactInteger(currentCorpusDf));
			}
		}

		@Override
		public void combine(Feature feature, Iterator<PactInteger> counts,
				Collector<Feature, PactInteger> out) {
			reduce(feature, counts, out);
		}
	}
	
	//TODO: Consider minDf && minSupport
	public static class DocumentFrequency extends ReduceStub<LabelFeaturePair, PactInteger, Label, FeatureCountPair> {
		@Override
		public void reduce(LabelFeaturePair labelFeature, Iterator<PactInteger> count,
				Collector<Label, FeatureCountPair> out) {
			int sum = 0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			FeatureCountPair featureCountPair = new FeatureCountPair();
			featureCountPair.setFirst(labelFeature.getSecond());
			featureCountPair.setSecond(new PactInteger(sum));
			out.collect(labelFeature.getFirst(), featureCountPair);
		}
	}
	
	//TODO: Consider minDf && minSupport
	@SameKey
	public static class NormalizedTf extends ReduceStub<LabelFeaturePair, PactDouble, LabelFeaturePair, PactDouble> {
		@Override
		public void reduce(LabelFeaturePair tokenPair, Iterator<PactDouble> count,
				Collector<LabelFeaturePair, PactDouble> out) {
			double sum = 0.0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			out.collect(tokenPair, new PactDouble(sum));
		}
	}
}
