package de.tu_berlin.dima.aim3.naivebayes;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class IdfCalculator extends MatchStub<PactPair<PactString, PactString>, PactDouble, PactDouble, PactPair<PactString, PactString>, PactDouble> {

	@Override
	public void match(PactPair<PactString, PactString> wordLabelPair, PactDouble weightFromWeightCalculator,
			PactDouble weightFromFeatureReducer,
			Collector<PactPair<PactString, PactString>, PactDouble> out) {
		double idfTimesDIJ = weightFromFeatureReducer.getValue() * weightFromWeightCalculator.getValue();
		out.collect(wordLabelPair, new PactDouble(idfTimesDIJ));
	}

}
