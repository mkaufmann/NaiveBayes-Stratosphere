/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tu_berlin.dima.aim3.naivebayes.io;

import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType1;
import static eu.stratosphere.pact.common.util.ReflectionUtil.getTemplateType2;

import java.io.DataOutputStream;
import java.io.IOException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Base implementation for delimiter based output formats. It can be adjusted to specific
 * formats by implementing the writeLine() method.
 * 
 * @author Moritz Kaufmann
 * @param <K>
 * @param <V>
 */
public abstract class BinaryOutputFormat<K extends Key, V extends Value> extends OutputFormat<K, V> {


	private DataOutputStream dataOutputStream;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void initTypes() {
		super.ok = getTemplateType1(getClass());
		super.ov = getTemplateType2(getClass());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open() {
		dataOutputStream = new DataOutputStream(super.stream);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writePair(KeyValuePair<K, V> pair) {
		try {
			pair.write(dataOutputStream);
		} catch (IOException e) {
			// TODO Log properly
			throw new RuntimeException("Should not happen", e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void configure(Configuration parameters) {
	}

}
