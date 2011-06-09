package de.tu_berlin.dima.aim3.naivebayes.io;

import java.io.IOException;
import java.net.URISyntaxException;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;

public class WeightInputFormat extends BinaryInputFormat<PactString, PactDouble> {

	public WeightInputFormat(String path) throws IOException,URISyntaxException {
		super(path);
	}

}
