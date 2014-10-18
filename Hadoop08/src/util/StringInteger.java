package util;

import java.io.DataInput;
import java.io.IOException;
import java.util.regex.Matcher;

public class StringInteger extends StringNumber {

	public StringInteger(String s, int t) {
		super(s,t);
	}

	public Integer getValue() {
		return (Integer) super.getValue();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		String indexStr = arg0.readUTF();

		Matcher m = p.matcher(indexStr);
		if (m.matches()) {
			super.s = m.group(1);
			super.t = Integer.parseInt(m.group(2));
		}
	}

}
