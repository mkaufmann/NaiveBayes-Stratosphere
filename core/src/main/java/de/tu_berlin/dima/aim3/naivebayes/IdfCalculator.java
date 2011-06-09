package de.tu_berlin.dima.aim3.naivebayes;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactDouble;

public class IdfCalculator extends MatchStub<LabelTokenPair, PactDouble, PactDouble, LabelTokenPair, PactDouble> {

	@Override
	public void match(LabelTokenPair wordLabelPair, PactDouble weightFromWeightCalculator,
			PactDouble weightFromFeatureReducer,
			Collector<LabelTokenPair, PactDouble> out) {
		double idfTimesDIJ = weightFromFeatureReducer.getValue() * weightFromWeightCalculator.getValue();
		out.collect(wordLabelPair, new PactDouble(idfTimesDIJ));
	}

}
