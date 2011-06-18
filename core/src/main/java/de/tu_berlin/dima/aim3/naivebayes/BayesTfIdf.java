package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.data.TokenCountPair;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesTfIdf {

	public static class OverallWordCountMapper extends MapStub<PactString, PactInteger, PactNull, PactInteger> {
		private final static PactInteger ONE = new PactInteger(1);
		
		@Override
		public void map(PactString word, PactInteger wordOccurences,
				Collector<PactNull, PactInteger> out) {
			out.collect(PactNull.getInstance(), ONE);
		}
	}


	@SameKey
	public static class IdfCalculator extends MatchStub<LabelFeaturePair, PactDouble, PactDouble, LabelFeaturePair, PactDouble> {

		@Override
		public void match(LabelFeaturePair wordLabelPair, PactDouble weightFromWeightCalculator,
				PactDouble weightFromFeatureReducer,
				Collector<LabelFeaturePair, PactDouble> out) {
			double idfTimesDIJ = weightFromFeatureReducer.getValue() * weightFromWeightCalculator.getValue();
			out.collect(wordLabelPair, new PactDouble(idfTimesDIJ));
		}

	}
	
	@Combinable
	@SameKey
	public static class OverallWordcountReducer extends ReduceStub<PactNull, PactInteger, PactNull, PactInteger> {

		@Override
		public void reduce(PactNull key, Iterator<PactInteger> wordCountIt,
				Collector<PactNull, PactInteger> out) {
			int sum = 0;
			while (wordCountIt.hasNext())
			{
				sum += wordCountIt.next().getValue();
			}
			out.collect(key, new PactInteger(sum));
		}

		@Override
		public void combine(PactNull key, Iterator<PactInteger> wordCountIt,
				Collector<PactNull, PactInteger> out) {
			reduce(key, wordCountIt, out);
		}
	}
	
	//@SuperKey ??
	public static class WeightCalculator extends MatchStub<PactString, PactInteger, TokenCountPair, LabelFeaturePair, PactDouble> {

		@Override
		public void match(PactString label, PactInteger trainerDocCount, TokenCountPair documentFrequency,
				Collector<LabelFeaturePair, PactDouble> out) {
	        double labelDocumentCount = (double)trainerDocCount.getValue();
	        double logIdf = Math.log(labelDocumentCount / (double)documentFrequency.getSecond().getValue());
	        out.collect(new LabelFeaturePair(label,documentFrequency.getFirst()), new PactDouble(logIdf));
		}

	}
	
}
