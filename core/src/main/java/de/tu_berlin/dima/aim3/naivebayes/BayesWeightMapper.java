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

import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactNull;

public class BayesWeightMapper {
	public static class FeatureSummer extends MapStub<LabelFeaturePair, PactDouble, Feature, PactDouble> {
		@Override
		public void map(LabelFeaturePair pair, PactDouble tfidf,
				Collector<Feature, PactDouble> out) {
			out.collect(pair.getSecond(), tfidf);
		}
	}
	
	public static class LabelSummer extends MapStub<LabelFeaturePair, PactDouble, Label, PactDouble> {
		@Override
		public void map(LabelFeaturePair pair, PactDouble tfidf,
				Collector<Label, PactDouble> out) {
			out.collect(pair.getFirst(), tfidf);
		}
	}
	
	public static class TotalSummer extends MapStub<LabelFeaturePair, PactDouble, PactNull, PactDouble> {
		@Override
		public void map(LabelFeaturePair pair, PactDouble tfidf,
				Collector<PactNull, PactDouble> out) {
			out.collect(PactNull.getInstance(), tfidf);
		}
	}
}
