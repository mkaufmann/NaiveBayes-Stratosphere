package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class BayesWeightReducer {
	@SameKey
	public static class Summer extends ReduceStub<PactString, PactDouble, PactString, PactDouble> {
		@Override
		public void reduce(PactString key, Iterator<PactDouble> values,
				Collector<PactString, PactDouble> out) {
			double sum = 0;
			while(values.hasNext()) {
				sum += values.next().getValue();
			}
			
			out.collect(key, new PactDouble(sum));
		}
	}
}
