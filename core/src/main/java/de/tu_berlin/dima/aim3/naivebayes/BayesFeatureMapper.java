package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Map.Entry;

import de.tu_berlin.dima.aim3.naivebayes.data.FeatureList;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import de.tu_berlin.dima.aim3.naivebayes.data.NormalizedTokenCountList;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesFeatureMapper  {
	private static final PactInteger INT_ONE = new PactInteger(1);
	private static final PactDouble DOUBLE_ONE = new PactDouble(1);
	
	private static int gramSize = 1;
	
	public static class Base extends MapStub<PactString, FeatureList, PactString, NormalizedTokenCountList> {
		@Override
		public void map(final PactString label, final FeatureList features,
				final Collector<PactString, NormalizedTokenCountList> out) {
			NormalizedTokenCountList tokenList = new NormalizedTokenCountList();
			
			//Count # of times a feature occurs in document
			if (gramSize > 1) {
				//TODO:!!!!!
			} else {
				for (PactString token : features) {
					if (tokenList.containsKey(token)) {
						tokenList.put(token, 1 + tokenList.get(token));
					} else {
						tokenList.put(token, 1);
					}
				}
			}
			
			// factor = sqrt((sum of all feature counts in documents)^2)
			double lengthNormalisationFactor = 0;
			for (Entry<PactString, Integer> entry : tokenList.entrySet()) {
				int tokenCount = entry.getValue();
				lengthNormalisationFactor += tokenCount * tokenCount;
			}
			lengthNormalisationFactor = Math.sqrt(lengthNormalisationFactor);
		    
			tokenList.setLengthNormalized(lengthNormalisationFactor);
			
			out.collect(label, tokenList);
		}
	}
	
	// Output Document Frequency per Word per Class
	public static class DocumentFrequency extends MapStub<PactString, NormalizedTokenCountList, LabelTokenPair, PactInteger> {		
		@Override
		public void map(PactString label, NormalizedTokenCountList tokenList,
				Collector<LabelTokenPair, PactInteger> out) {
			for (Entry<PactString, Integer> entry : tokenList.entrySet()) {
				PactString token = entry.getKey();
				
				LabelTokenPair dfTuple = new LabelTokenPair();
		        dfTuple.setFirst(label);
		        dfTuple.setSecond(token);
		        out.collect(dfTuple, INT_ONE);
			}
		}
	}
	
	// Corpus Document Frequency (FEATURE_COUNT)
	public static class FeatureCount extends MapStub<PactString, NormalizedTokenCountList, PactString, PactDouble> {		
		@Override
		public void map(PactString label, NormalizedTokenCountList tokenList,
				Collector<PactString, PactDouble> out) {
			for (Entry<PactString, Integer> entry : tokenList.entrySet()) {
				PactString token = entry.getKey();
		        out.collect(token, DOUBLE_ONE);
			}
		}

	}
	
	// Corpus Term Frequency (FEATURE_TF)
	public static class FeatureTf extends MapStub<PactString, NormalizedTokenCountList, PactString, PactDouble> {
		@Override
		public void map(PactString label, NormalizedTokenCountList tokenList,
				Collector<PactString, PactDouble> out) {
			for (Entry<PactString, Integer> entry : tokenList.entrySet()) {
				PactString token = entry.getKey();
				out.collect(token, new PactDouble(tokenList.getLengthNormalized()));
			}
		}

	}

	public static class LabelCount extends MapStub<PactString, NormalizedTokenCountList, PactString, PactInteger> {
		@Override
		public void map(PactString label, NormalizedTokenCountList tokenList,
				Collector<PactString, PactInteger> out) {
		    out.collect(label, INT_ONE);
		}

	}
	
    // Output Length Normalized + TF Transformed Frequency per Word per Class
    // Log(1 + D_ij)/SQRT( SIGMA(k, D_kj) )
	public static class Weight extends MapStub<PactString, NormalizedTokenCountList, LabelTokenPair, PactDouble> {
		@Override
		public void map(PactString label, NormalizedTokenCountList tokenList,
				Collector<LabelTokenPair, PactDouble> out) {
			for (Entry<PactString, Integer> entry : tokenList.entrySet()) {
				PactString token = entry.getKey();
				int tokenCount = entry.getValue();
				
				LabelTokenPair tuple = new LabelTokenPair();
				tuple.setFirst(label);
				tuple.setSecond(token);
				
				PactDouble f = new PactDouble(Math.log(1.0 + tokenCount) / tokenList.getLengthNormalized());
				out.collect(tuple, f);
			}
		}

	}

}
