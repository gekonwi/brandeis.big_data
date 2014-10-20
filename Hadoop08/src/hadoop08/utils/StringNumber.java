package hadoop08.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public abstract class StringNumber<V extends Number> implements Writable {

	protected String string;
	protected V value;
	public static Pattern p = Pattern.compile("(.+),([0-9]{1,13}(\\.[0-9]*)?)");

	public StringNumber() {
	}

	public StringNumber(String string, V value) {
		this.string = string;
		this.value = value;
	}

	public String getString() {
		return string;
	}

	public V getValue() {
		return value;
	}

	@Override
	abstract public void readFields(DataInput arg0) throws IOException;

	@Override
	public void write(DataOutput arg0) throws IOException {
		StringBuffer sb = new StringBuffer();
		sb.append(string);
		sb.append(",");
		sb.append(value);
		arg0.writeUTF(sb.toString());
	}

	@Override
	public String toString() {
		return string + "," + value;
	}

}
