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
