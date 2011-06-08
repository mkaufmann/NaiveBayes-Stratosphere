package de.tu_berlin.dima.aim3.naivebayes;

import java.util.HashSet;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

@Combinable
public class UniqueIdentifierReducer extends ReduceStub<PactInteger, PactString, PactString, PactInteger> {

	private final static PactInteger PACT_ONE = new PactInteger(1);
	
	@Override
	public void reduce(PactInteger key, Iterator<PactString> values,
			Collector<PactString, PactInteger> out) {

		int i = 0;
		HashSet<PactString> hashSet = new HashSet<PactString>();
		while (values.hasNext())
		{
			hashSet.add(values.next());
		}
		Iterator<PactString> it = hashSet.iterator();
		while (it.hasNext())
		{
			PactString asd = it.next();
			//LOG.info(asd);
			out.collect(asd, new PactInteger(i++));
		}
		
	}
	
	@Override
	public void combine(PactInteger key, Iterator<PactString> values,
			Collector<PactInteger, PactString> out) {

		HashSet<PactString> hashSet = new HashSet<PactString>();
		while (values.hasNext())
		{
			hashSet.add(values.next());
		}
		Iterator<PactString> it = hashSet.iterator();
		while (it.hasNext())
		{
			out.collect(PACT_ONE, it.next());
		}
	}

}
