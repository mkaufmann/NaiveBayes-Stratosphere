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
	
	public static class TotalDistinctFeatureCountMapper extends MapStub<PactString, PactInteger, PactNull, PactInteger> {
		private final static PactInteger ONE = new PactInteger(1);
		
		@Override
		public void map(PactString feature, PactInteger frequency,
				Collector<PactNull, PactInteger> out) {
			out.collect(PactNull.getInstance(), ONE);
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
	
	//@SuperKey ??
	public static class IdfCalculator extends MatchStub<PactString, PactInteger, TokenCountPair, LabelFeaturePair, PactDouble> {

		@Override
		public void match(PactString label, PactInteger labelDocCount, TokenCountPair df,
				Collector<LabelFeaturePair, PactDouble> out) {
	        int labelDocumentCount = labelDocCount.getValue();
	        double logIdf = Math.log(labelDocumentCount / (double)df.getSecond().getValue());
	        out.collect(new LabelFeaturePair(label,df.getFirst()), new PactDouble(logIdf));
		}

	}
	
}
