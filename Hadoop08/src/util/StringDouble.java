package util;

import java.io.DataInput;
import java.io.IOException;
import java.util.regex.Matcher;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class StringDouble extends StringNumber<Double> {

	public StringDouble(String string, double value) {
		super(string, value);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		String indexStr = arg0.readUTF();

		Matcher m = p.matcher(indexStr);
		if (m.matches()) {
			super.string = m.group(1);
			super.value = Double.parseDouble(m.group(2));
		}
	}

}
