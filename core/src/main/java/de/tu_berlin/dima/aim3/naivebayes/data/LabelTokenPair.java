package de.tu_berlin.dima.aim3.naivebayes.data;

import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class LabelTokenPair extends PactPair<PactString, PactString>{

	public LabelTokenPair(PactString first, PactString second) {
		super(first,second);
	}
	
	public LabelTokenPair(){
		super();
	}

}
