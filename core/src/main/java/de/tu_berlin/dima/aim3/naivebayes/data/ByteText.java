package de.tu_berlin.dima.aim3.naivebayes.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import eu.stratosphere.pact.common.type.Key;

public class ByteText implements Key {
	byte[] value;
	
	public ByteText(byte[] bytes) {
		this.value = bytes;
	}
	
	public ByteText() {
		
	}

	@Override
	public void read(DataInput in) throws IOException {
		//value array is never allowed to be null!!
		int length = in.readInt();
		value = new byte[length];
		in.readFully(value);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//value array is never allowed to be null!!
		out.writeInt(value.length);
		out.write(value);
	}

	@Override
	public int compareTo(Key o) {
		//Sorts by length first and than by content
		ByteText other = (ByteText) o;
		
		//value array is never allowed to be null!!
		int diff = value.length - other.value.length;
		if(diff != 0) {
			return diff;
		}
		else {
			//Both have same length
			for (int i = 0; i < value.length; i++) {
				if(value[i] - other.value[i] != 0) {
					return value[i] - other.value[i];
				}
			}
		}
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		return new String(value);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(value);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		ByteText other = (ByteText) obj;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}
}
