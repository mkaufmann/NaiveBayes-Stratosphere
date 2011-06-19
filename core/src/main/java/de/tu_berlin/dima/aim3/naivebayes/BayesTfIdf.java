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
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.data.FeatureCountPair;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;

public class BayesTfIdf {
	
	public static class TotalDistinctFeatureCountMapper extends MapStub<Feature, PactInteger, PactNull, PactInteger> {
		private final static PactInteger ONE = new PactInteger(1);
		
		@Override
		public void map(Feature feature, PactInteger frequency,
				Collector<PactNull, PactInteger> out) {
			out.collect(PactNull.getInstance(), ONE);
		}
	}
	
	@Combinable
	@SameKey
	public static class TotalDistinctFeatureCountReducer extends ReduceStub<PactNull, PactInteger, PactNull, PactInteger> {

		@Override
		public void reduce(PactNull key, Iterator<PactInteger> count,
				Collector<PactNull, PactInteger> out) {
			int sum = 0;
			
			while (count.hasNext())
			{
				sum += count.next().getValue();
			}
			
			out.collect(key, new PactInteger(sum));
		}

		@Override
		public void combine(PactNull key, Iterator<PactInteger> wordCountIt,
				Collector<PactNull, PactInteger> out) {
			reduce(key, wordCountIt, out);
		}
	}


	@SameKey
	public static class TfIdfCalculator extends MatchStub<LabelFeaturePair, PactDouble, PactDouble, LabelFeaturePair, PactDouble> {

		@Override
		public void match(LabelFeaturePair labelFeature, PactDouble weightFromWeightCalculator,
				PactDouble weightFromFeatureReducer,
				Collector<LabelFeaturePair, PactDouble> out) {
			double idfTimesDIJ = weightFromFeatureReducer.getValue() * weightFromWeightCalculator.getValue();
			out.collect(labelFeature, new PactDouble(idfTimesDIJ));
		}

	}
	
	//@SuperKey ??
	public static class IdfCalculator extends MatchStub<Label, PactInteger, FeatureCountPair, LabelFeaturePair, PactDouble> {

		@Override
		public void match(Label label, PactInteger labelDocCount, FeatureCountPair df,
				Collector<LabelFeaturePair, PactDouble> out) {
	        int labelDocumentCount = labelDocCount.getValue();
	        double logIdf = Math.log(labelDocumentCount / (double)df.getSecond().getValue());
	        out.collect(new LabelFeaturePair(label,df.getFirst()), new PactDouble(logIdf));
		}

	}
	
}
