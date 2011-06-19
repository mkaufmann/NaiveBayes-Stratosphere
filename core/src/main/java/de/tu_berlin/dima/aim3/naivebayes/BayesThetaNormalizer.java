package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import de.tu_berlin.dima.aim3.naivebayes.data.ThetaNormalizerFactors;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;

public class BayesThetaNormalizer {
	private static double alphaI = 1.0;
	
	public static class ThetaFactorsVocabCountSigmaJSigmaK extends CrossStub<PactNull, PactInteger, PactNull, PactDouble, PactNull, ThetaNormalizerFactors> {
		@Override
		public void cross(PactNull keyA, PactInteger vocabCount, PactNull keyB,
				PactDouble sigmaJSigmaK,
				Collector<PactNull, ThetaNormalizerFactors> out) {
			ThetaNormalizerFactors factors = new ThetaNormalizerFactors();
			factors.setSigmaJSimgaK(sigmaJSigmaK.getValue());
			factors.setVocabCount(vocabCount.getValue());
			out.collect(PactNull.getInstance(), factors);
		}
	}
	
	public static class ThetaFactorsLabelWeights extends CrossStub<PactNull, ThetaNormalizerFactors, Label, PactDouble, Label, ThetaNormalizerFactors> {
		@Override
		public void cross(PactNull keyA, ThetaNormalizerFactors factors, Label label,
				PactDouble weight,
				Collector<Label, ThetaNormalizerFactors> out) {
			factors.setLabelWeight(weight.getValue());
			out.collect(label, factors);
		}
	}
	
	public static class TfIdfTransform extends MapStub<LabelFeaturePair, PactDouble, Label, PactDouble> {
		@Override
		public void map(LabelFeaturePair labelToken, PactDouble tfidf,
				Collector<Label, PactDouble> out) {
			out.collect(labelToken.getFirst(), tfidf);
		}
	}
	
	@SameKey
	public static class ThetaNormalize extends CoGroupStub<Label, PactDouble, ThetaNormalizerFactors, Label, PactDouble> {		
		@Override
		public void coGroup(Label label, Iterator<PactDouble> tfidfs,
				Iterator<ThetaNormalizerFactors> factors,
				Collector<Label, PactDouble> out) {
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
