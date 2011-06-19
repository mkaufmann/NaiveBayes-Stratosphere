package de.tu_berlin.dima.aim3.naivebayes.data;

import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactPair;

public class FeatureCountPair extends PactPair<Feature, PactInteger> {
	
	public FeatureCountPair(Feature word, PactInteger count)
	{
		super(word, count);
	}

	public FeatureCountPair()
	{
		super();
	}
}
