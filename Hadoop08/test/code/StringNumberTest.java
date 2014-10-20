package code;

import static org.junit.Assert.assertEquals;
import hadoop08.util.StringDouble;
import hadoop08.util.StringInteger;

import java.io.IOException;

import org.junit.Test;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class StringNumberTest {

	private static final double DOUBLE_COURTESY = 0.00001;

	@Test
	public void testStringInteger() throws IOException {
		final String string = "StringIntegerTest1";
		final int value = 100;
		StringInteger si = new StringInteger(string, value);

		assertEquals(string, si.getString());
		assertEquals(value, (int) si.getValue());
		assertEquals(string + "," + value, si.toString());
	}

	@Test
	public void testStringDouble1() {
		testStringDouble("StringDoubleTest1", 100.001);
	}

	/**
	 * test out double without a leading 0 and is < 1
	 */
	@Test
	public void testStringDouble2() {
		testStringDouble("StringDoubleTest2", .1111);
	}

	private void testStringDouble(String string, double value) {
		StringDouble sd = new StringDouble(string, value);

		assertEquals(string, sd.getString());

		assertEquals(value, sd.getValue(), DOUBLE_COURTESY);

		assertEquals(string + "," + value, sd.toString());
	}

	@Test
	// will need implementation after figuring out how to pass in readFields
	// parameters correctly
	public void testStringIntegerReadFields() {

	}

	@Test
	// will need implementation after figuring out how to pass in readFields
	// parameters correctly
	public void testStringDoubleReadFields() {
	}

}
