package de.tu_berlin.dima.aim3.naivebayes.io;

import de.tu_berlin.dima.aim3.naivebayes.data.Feature;
import de.tu_berlin.dima.aim3.naivebayes.data.Label;
import de.tu_berlin.dima.aim3.naivebayes.data.LabelFeaturePair;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactNull;

public class BayesOutputFormats {
	
	public static class TotalSumOutputFormat extends BinaryOutputFormat<PactNull, PactDouble> {	}
	
	public static class FeatureSumOutputFormat extends BinaryOutputFormat<Feature, PactDouble> { }
	
	public static class LabelSumOutputFormat extends BinaryOutputFormat<Label, PactDouble> {	}
	
	public static class IdfOutputFormat extends BinaryOutputFormat<LabelFeaturePair, PactDouble> {}
	
	public static class ThetaNormalizedOutputFormat extends BinaryOutputFormat<Label, PactDouble> {}
	
}
