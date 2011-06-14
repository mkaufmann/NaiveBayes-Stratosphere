package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import de.tu_berlin.dima.aim3.naivebayes.data.TokenCountPair;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesFeatureReducer {
	private static double minDf = -1;
	
	@SameKey
	public static class LabelCount extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {
		@Override
		public void reduce(PactString label, Iterator<PactInteger> count,
				Collector<PactString, PactInteger> out) {
			int sum = 0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			out.collect(label, new PactInteger(sum));
		}
	}
	
	@SameKey
	public static class FeatureTf extends ReduceStub<PactString, PactDouble, PactString, PactDouble> {
		@Override
		public void reduce(PactString feature, Iterator<PactDouble> count,
				Collector<PactString, PactDouble> out) {
			double sum = 0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			out.collect(feature, new PactDouble(sum));
		}
	}
	
	@SameKey
	public static class FeatureCount extends ReduceStub<PactString, PactDouble, PactString, PactDouble> {
		@Override
		public void reduce(PactString feature, Iterator<PactDouble> count,
				Collector<PactString, PactDouble> out) {
			double currentCorpusDf = 0;
			while(count.hasNext()) {
				currentCorpusDf += count.next().getValue();
			}
			
			if (minDf > 0.0 && currentCorpusDf < minDf) {
				System.out.println("Skipped " + feature.getValue() + " less than minDf");
				// skip items that have less than the specified minSupport.
			} else {
				out.collect(feature, new PactDouble(currentCorpusDf));
			}
		}
	}
	
	//TODO: Consider minDf && minSupport
	public static class DocumentFrequency extends ReduceStub<LabelTokenPair, PactInteger, PactString, TokenCountPair> {
		@Override
		public void reduce(LabelTokenPair tokenPair, Iterator<PactInteger> count,
				Collector<PactString, TokenCountPair> out) {
			int sum = 0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			TokenCountPair tokenCountPair = new TokenCountPair();
			tokenCountPair.setFirst(tokenPair.getSecond());
			tokenCountPair.setSecond(new PactInteger(sum));
			out.collect(tokenPair.getFirst(), tokenCountPair);
		}
	}
	
	//TODO: Consider minDf && minSupport
	@SameKey
	public static class Weight extends ReduceStub<LabelTokenPair, PactDouble, LabelTokenPair, PactDouble> {
		@Override
		public void reduce(LabelTokenPair tokenPair, Iterator<PactDouble> count,
				Collector<LabelTokenPair, PactDouble> out) {
			double sum = 0.0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			out.collect(tokenPair, new PactDouble(sum));
		}
	}
}
