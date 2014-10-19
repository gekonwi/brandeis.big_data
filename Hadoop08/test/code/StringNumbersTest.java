package code;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import util.StringDouble;
import util.StringInteger;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class StringNumbersTest {

	@Test
	public void testStringInteger() throws IOException {
		StringInteger si_1 = new StringInteger("StringIntegerTest1", 100);

		assertEquals("StringIntegerTest1", si_1.getString());

		assertEquals((Integer) 100, si_1.getValue());

		assertEquals("StringIntegerTest1,100", si_1.toString());
	}

	@Test
	public void testStringDouble() {
		StringDouble sd_1 = new StringDouble("StringDoubleTest1", 100.001);
		// 2nd StringDouble test is to test out double without a leading 0 and
		// is < 1
		StringDouble sd_2 = new StringDouble("StringDoubleTest2", .1111);

		assertEquals("StringDoubleTest1", sd_1.getString());

		assertEquals((Double) 100.001, sd_1.getValue());

		assertEquals("StringDoubleTest1,100.001", sd_1.toString());

		assertEquals("StringDoubleTest2", sd_2.getString());

		assertEquals((Double) .1111, sd_2.getValue());

		assertEquals("StringDoubleTest2,0.1111", sd_2.toString());
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
