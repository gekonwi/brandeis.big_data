package hadoop08.util;

import java.io.DataInput;
import java.io.IOException;
import java.util.regex.Matcher;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class StringInteger extends StringNumber<Integer> {

	// required for reflection instantiation in Hadoop
	public StringInteger() {
	}

	public StringInteger(String string, int value) {
		super(string, value);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		String indexStr = arg0.readUTF();

		Matcher m = p.matcher(indexStr);
		if (m.matches()) {
			super.string = m.group(1);
			super.value = Integer.parseInt(m.group(2));
		}
	}

}
