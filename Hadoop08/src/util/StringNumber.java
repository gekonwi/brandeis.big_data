package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;

public abstract class StringNumber implements Writable {
	
	protected String s;
	protected Number t;
	public static Pattern p = Pattern.compile("(.+),([0-9]{1,13}(\\.[0-9]*)?)");

	public StringNumber() {
	}

	public StringNumber(String s, Number t) {
		this.s = s;
		this.t = t;
	}

	public String getString() {
		return s;
	}

	public Number getValue() {
		return t;
	}

	@Override
	abstract public void readFields(DataInput arg0) throws IOException;

	@Override
	public void write(DataOutput arg0) throws IOException {
		StringBuffer sb = new StringBuffer();
		sb.append(s);
		sb.append(",");
		sb.append(t);
		arg0.writeUTF(sb.toString());
	}

	@Override
	public String toString() {
		return s + "," + t;
	}
	
}
