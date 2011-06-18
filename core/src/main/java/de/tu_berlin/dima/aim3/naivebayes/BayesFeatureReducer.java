package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.data.TokenCountPair;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesFeatureReducer {
	private static double minDf = -1;
	
	
	/**
	 * Count # of occurrences per label in all documents.
	 * 
	 * Sums up # of occurrences for a specific label.
	 * @author mkaufmann
	 *
	 */
	@SameKey
	@Combinable
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

		@Override
		public void combine(PactString label, Iterator<PactInteger> counts,
				Collector<PactString, PactInteger> out) {
			reduce(label, counts, out);
		}
	}
	
	@SameKey
	@Combinable
	public static class FeatureTf extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {
		@Override
		public void reduce(PactString feature, Iterator<PactInteger> count,
				Collector<PactString, PactInteger> out) {
			int sum = 0;
			while(count.hasNext()) {
				sum += count.next().getValue();
			}
			
			out.collect(feature, new PactInteger(sum));
		}

		@Override
		public void combine(PactString feature, Iterator<PactInteger> counts,
				Collector<PactString, PactInteger> out) {
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
	public static class FeatureCount extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {
		@Override
		public void reduce(PactString feature, Iterator<PactInteger> count,
				Collector<PactString, PactInteger> out) {
			int currentCorpusDf = 0;
			while(count.hasNext()) {
				currentCorpusDf += count.next().getValue();
			}
			
			if (minDf > 0.0 && currentCorpusDf < minDf) {
				System.out.println("Skipped " + feature.getValue() + " less than minDf");
			} else {
				out.collect(feature, new PactInteger(currentCorpusDf));
			}
		}

		@Override
		public void combine(PactString feature, Iterator<PactInteger> counts,
				Collector<PactString, PactInteger> out) {
			reduce(feature, counts, out);
		}
	}
	
	//TODO: Consider minDf && minSupport
	public static class DocumentFrequency extends ReduceStub<LabelFeaturePair, PactInteger, PactString, TokenCountPair> {
		@Override
		public void reduce(LabelFeaturePair tokenPair, Iterator<PactInteger> count,
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
	public static class Weight extends ReduceStub<LabelFeaturePair, PactDouble, LabelFeaturePair, PactDouble> {
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
