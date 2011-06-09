package de.tu_berlin.dima.aim3.naivebayes.io;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesOutputFormats {
	
	public static class WeightOutputFormat extends BinaryOutputFormat<PactString, PactDouble> {	}
	
	public static class IdfOutputFormat extends BinaryOutputFormat<LabelTokenPair, PactDouble> {}
	
	public static class ThetaNormalizedOutputFormat extends BinaryOutputFormat<PactString, PactDouble> {}
	
}