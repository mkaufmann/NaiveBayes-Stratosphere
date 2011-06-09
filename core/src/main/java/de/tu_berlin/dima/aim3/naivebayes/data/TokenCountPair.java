package de.tu_berlin.dima.aim3.naivebayes.data;

import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class TokenCountPair extends PactPair<PactString, PactInteger> {
	
	public TokenCountPair(PactString word, PactInteger count)
	{
		super(word, count);
	}

	public TokenCountPair()
	{
		super();
	}
}
