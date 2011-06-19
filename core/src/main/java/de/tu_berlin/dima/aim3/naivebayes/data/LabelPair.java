package de.tu_berlin.dima.aim3.naivebayes.data;

import eu.stratosphere.pact.common.type.base.PactPair;

public class LabelPair extends PactPair<Label, Label>{

	public LabelPair(Label correct, Label classified) {
		super(correct,classified);
	}
	
	public LabelPair(){
		super();
	}

}
