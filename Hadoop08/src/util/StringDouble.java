package util;

import java.io.DataInput;
import java.io.IOException;
import java.util.regex.Matcher;

public class StringDouble extends StringNumber {

	public StringDouble(String s, double t) {
		super(s, t);
	}

	public Double getValue() {
		return (Double) super.getValue();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		String indexStr = arg0.readUTF();

		Matcher m = p.matcher(indexStr);
		if (m.matches()) {
			super.s = m.group(1);
			super.t = Double.parseDouble(m.group(2));
		}
	}
	
}
