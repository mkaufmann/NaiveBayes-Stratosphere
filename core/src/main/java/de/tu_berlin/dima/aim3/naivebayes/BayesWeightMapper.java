package de.tu_berlin.dima.aim3.naivebayes;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesWeightMapper {
	public static class FeatureSummer extends MapStub<LabelTokenPair, PactDouble, PactString, PactDouble> {
		@Override
		public void map(LabelTokenPair pair, PactDouble tfidf,
				Collector<PactString, PactDouble> out) {
			out.collect(pair.getSecond(), tfidf);
		}
	}
	
	public static class LabelSummer extends MapStub<LabelTokenPair, PactDouble, PactString, PactDouble> {
		@Override
		public void map(LabelTokenPair pair, PactDouble tfidf,
				Collector<PactString, PactDouble> out) {
			out.collect(pair.getFirst(), tfidf);
		}
	}
	
	public static class TotalSummer extends MapStub<LabelTokenPair, PactDouble, PactString, PactDouble> {
		private static PactString EMPTY = new PactString();
		@Override
		public void map(LabelTokenPair pair, PactDouble tfidf,
				Collector<PactString, PactDouble> out) {
			out.collect(EMPTY, tfidf);
		}
	}
}
