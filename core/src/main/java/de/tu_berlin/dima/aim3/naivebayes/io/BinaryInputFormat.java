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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public abstract class BinaryInputFormat<K extends Key, V extends Value> {
	
	protected DataInputStream dataInputStream;
	
	public BinaryInputFormat(String path) throws IOException, URISyntaxException {

		FileSystem fileSystem = FileSystem.get(new URI(path));
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path(path));
		dataInputStream = new DataInputStream(fsDataInputStream);
	}
	
	public boolean readPair(K keyHolder, V valueHolder) throws IOException
	{
		try
		{
			keyHolder.read(dataInputStream);
			valueHolder.read(dataInputStream);
			return true;
		}
		catch (EOFException e) {
			dataInputStream.close();
			return false;
		}
	}

}
