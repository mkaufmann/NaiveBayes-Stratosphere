package de.tu_berlin.dima.aim3.naivebayes.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.pact.common.type.Key;

public class ByteText implements Key {
	//byte[] value;
	String value;
	
	public ByteText(byte[] bytes) {
		this.value = new String(bytes);
	}
	
	public ByteText() {
		
	}

	@Override
	public void read(DataInput in) throws IOException {
		value = in.readUTF();  
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(value);
	}

	@Override
	public int compareTo(Key o) {
		//Sorts by length first and than by content
		
		return value.compareTo(((ByteText)o).value);
	}
	
	@Override
	public String toString() {
		return new String(value);
	}

	@Override
	public int hashCode() {
		return value.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return value.equals(((ByteText)obj).value);
	}
}
