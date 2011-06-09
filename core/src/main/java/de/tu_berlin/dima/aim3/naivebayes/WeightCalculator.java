package de.tu_berlin.dima.aim3.naivebayes;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelTokenPair;
import de.tu_berlin.dima.aim3.naivebayes.data.TokenCountPair;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;


/**
 * 
 * Calculate weight
 * 
 * @author ringwald
 *
 */
public class WeightCalculator extends MatchStub<PactString, PactDouble, TokenCountPair, LabelTokenPair, PactDouble> {

	@Override
	public void match(PactString label, PactDouble trainerDocCount, TokenCountPair documentFrequency,
			Collector<LabelTokenPair, PactDouble> out) {
        Double labelDocumentCount = trainerDocCount.getValue();
        double logIdf = Math.log(labelDocumentCount / documentFrequency.getSecond().getValue());
        out.collect(new LabelTokenPair(label,documentFrequency.getFirst()) {}, new PactDouble(logIdf));
	}

}
