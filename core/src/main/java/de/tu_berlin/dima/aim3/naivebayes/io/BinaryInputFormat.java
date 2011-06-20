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
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public abstract class BinaryInputFormat<K extends Key, V extends Value> {
	
	private static final Log LOG = LogFactory.getLog(BinaryInputFormat.class);

	
	protected DataInputStream dataInputStream;
	
	private Queue<FileStatus> files = new LinkedList<FileStatus>();
	private FileSystem fs;
	
	public BinaryInputFormat(String pathString) throws IOException, URISyntaxException {

		fs = FileSystem.get(new URI(pathString));
		Path path = new Path(pathString);
		final FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			// input is directory. list all contained files
			final FileStatus[] dir = fs.listStatus(path);
			for (int i = 0; i < dir.length; i++) {
				if (!dir[i].isDir()) {
					String fileName = dir[i].getPath().getName();
					if (fileName.startsWith(".") == false)
					{
						files.add(dir[i]);						
					}
				}
			}

		} else {
			files.add(pathFile);
		}
		getNextReader();
	}
	
	private boolean getNextReader()
	{
		if (files.isEmpty() == false)
		{
			FileStatus fileStatus = files.poll();
			FSDataInputStream fsDataInputStream;
			try {
				LOG.info("Opening reader :" + fileStatus.getPath());
				fsDataInputStream = fs.open(fileStatus.getPath());
				dataInputStream = new DataInputStream(fsDataInputStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
			return true;
		}
		else
		{
			return false;
		}
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
			LOG.info("Data input stream closed");
			if (getNextReader() == true)
			{
				return readPair(keyHolder, valueHolder);
			}
			return false;
		}
	}

}
