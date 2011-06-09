package de.tu_berlin.dima.aim3.naivebayes.classifier;

import java.util.Iterator;

import de.tu_berlin.dima.aim3.naivebayes.data.LabelPair;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Summing reducer
 * 
 * @author mkaufmann
 *
 */
public class ClassifyingReducer extends ReduceStub<LabelPair, PactInteger, LabelPair, PactInteger> {

	@Override
	public void reduce(LabelPair labels, Iterator<PactInteger> counts,
			Collector<LabelPair, PactInteger> out) {
		int sum = 0;
		
	    while (counts.hasNext()) {
	      sum += counts.next().getValue();
	    }
	    
	    out.collect(labels, new PactInteger(sum));
	}

}
