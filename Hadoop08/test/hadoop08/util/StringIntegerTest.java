package hadoop08.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class StringIntegerTest {

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
	// TODO will need implementation after figuring out how to pass in
	// readFields
	// parameters correctly
	public void testStringIntegerReadFields() {

	}
}
