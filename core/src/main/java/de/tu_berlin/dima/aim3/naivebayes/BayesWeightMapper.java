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
