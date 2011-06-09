package de.tu_berlin.dima.aim3.naivebayes;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

@Combinable
public class OverallWordcountReducer extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {

	@Override
	public void reduce(PactInteger nonsenseUniqueKey, Iterator<PactInteger> wordCountIt,
			Collector<PactInteger, PactInteger> out) {
		int sum = 0;
		while (wordCountIt.hasNext())
		{
			sum += wordCountIt.next().getValue();
		}
		out.collect(nonsenseUniqueKey, new PactInteger(sum));
	}

	@Override
	public void combine(PactInteger nonsenseUniqueKey, Iterator<PactInteger> wordCountIt,
			Collector<PactInteger, PactInteger> out) {
		int sum = 0;
		while (wordCountIt.hasNext())
		{
			sum += wordCountIt.next().getValue();
		}
		out.collect(nonsenseUniqueKey, new PactInteger(sum));
	}
}
