package de.tu_berlin.dima.aim3.naivebayes;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;


/**
 * 
 * Calculate weight
 * 
 * @author ringwald
 *
 */
public class WeightCalculator extends MatchStub<PactString, PactDouble, PactPair<PactString, PactDouble>, PactPair<PactString,PactString>, PactDouble> {

	@Override
	public void match(PactString label, PactDouble trainerDocCount, PactPair<PactString, PactDouble> documentFrequency,
			Collector<PactPair<PactString,PactString>, PactDouble> out) {
        Double labelDocumentCount = trainerDocCount.getValue();
        double logIdf = Math.log(labelDocumentCount / documentFrequency.getSecond().getValue());
        out.collect(new PactPair<PactString, PactString>(label,documentFrequency.getFirst()) {}, new PactDouble(logIdf));
	}

}
