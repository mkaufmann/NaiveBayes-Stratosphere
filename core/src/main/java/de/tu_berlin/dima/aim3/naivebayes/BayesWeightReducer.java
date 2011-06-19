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

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactNull;

public class BayesWeightReducer {
	
	@SameKey
	@Combinable
	public static class FeatureSummer extends ReduceStub<Feature, PactDouble, Feature, PactDouble> {
		@Override
		public void reduce(Feature feature, Iterator<PactDouble> values,
				Collector<Feature, PactDouble> out) {
			double sum = 0;
			while(values.hasNext()) {
				sum += values.next().getValue();
			}
			
			out.collect(feature, new PactDouble(sum));
		}

		@Override
		public void combine(Feature key, Iterator<PactDouble> values,
				Collector<Feature, PactDouble> out) {
			reduce(key, values, out);
		}
	}
	
	@SameKey
	@Combinable
	public static class LabelSummer extends ReduceStub<Label, PactDouble, Label, PactDouble> {
		@Override
		public void reduce(Label feature, Iterator<PactDouble> values,
				Collector<Label, PactDouble> out) {
			double sum = 0;
			while(values.hasNext()) {
				sum += values.next().getValue();
			}
			
			out.collect(feature, new PactDouble(sum));
		}

		@Override
		public void combine(Label key, Iterator<PactDouble> values,
				Collector<Label, PactDouble> out) {
			reduce(key, values, out);
		}
	}
	
	@SameKey
	@Combinable
	public static class NullSummer extends ReduceStub<PactNull, PactDouble, PactNull, PactDouble> {
		@Override
		public void reduce(PactNull key, Iterator<PactDouble> values,
				Collector<PactNull, PactDouble> out) {
			double sum = 0;
			while(values.hasNext()) {
				sum += values.next().getValue();
			}
			
			out.collect(key, new PactDouble(sum));
		}

		@Override
		public void combine(PactNull key, Iterator<PactDouble> values,
				Collector<PactNull, PactDouble> out) {
			reduce(key, values, out);
		}
	}
}
