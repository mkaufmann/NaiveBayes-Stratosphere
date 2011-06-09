package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import de.tu_berlin.dima.aim3.naivebayes.data.ThetaNormalizerFactors;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesThetaNormalizer {
	private static double alphaI = 1.0;
	
	public static class ThetaFactorsVocabCountSigmaJSigmaK extends CrossStub<PactInteger, PactInteger, PactString, PactDouble, PactNull, ThetaNormalizerFactors> {
		@Override
		public void cross(PactInteger keyA, PactInteger vocabCount, PactString keyB,
				PactDouble sigmaJSigmaK,
				Collector<PactNull, ThetaNormalizerFactors> out) {
			ThetaNormalizerFactors factors = new ThetaNormalizerFactors();
			factors.setSigmaJSimgaK(sigmaJSigmaK.getValue());
			factors.setVocabCount(vocabCount.getValue());
			out.collect(PactNull.getInstance(), factors);
		}
	}
	
	public static class ThetaFactorsLabelWeights extends CrossStub<PactNull, ThetaNormalizerFactors, PactString, PactDouble, PactString, ThetaNormalizerFactors> {
		@Override
		public void cross(PactNull keyA, ThetaNormalizerFactors factors, PactString label,
				PactDouble weight,
				Collector<PactString, ThetaNormalizerFactors> out) {
			factors.setLabelWeight(weight.getValue());
			out.collect(label, factors);
		}
	}
	
	public static class TfIdfTransform extends MapStub<LabelTokenPair, PactDouble, PactString, PactDouble> {
		@Override
		public void map(LabelTokenPair labelToken, PactDouble tfidf,
				Collector<PactString, PactDouble> out) {
			out.collect(labelToken.getFirst(), tfidf);
		}
	}
	
	public static class ThetaNormalize extends CoGroupStub<PactString, PactDouble, ThetaNormalizerFactors, PactString, PactDouble> {		
		@Override
		public void coGroup(PactString label, Iterator<PactDouble> tfidfs,
				Iterator<ThetaNormalizerFactors> factors,
				Collector<PactString, PactDouble> out) {
			ThetaNormalizerFactors factor = factors.next();
			
			if(factors.hasNext()) {
				throw new RuntimeException("Something's wrong in here!");
			}
			
			double sum = 0;
			while(tfidfs.hasNext()) {
				double tfidf = tfidfs.next().getValue();
				double normalized = Math.log((tfidf + alphaI) / factor.getLabelWeight() + factor.getVocabCount());
				sum += normalized;
			}
			
			out.collect(label, new PactDouble(sum));
		}
	}
}
