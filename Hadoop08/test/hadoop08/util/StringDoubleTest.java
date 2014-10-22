package hadoop08.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class StringDoubleTest {

	private static final double DOUBLE_COURTESY = 0.00001;

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
	// TODO will need implementation after figuring out how to pass in
	// readFields
	// parameters correctly
	public void testStringDoubleReadFields() {
	}

	/**
	 * Required in
	 * org.apache.hadoop.util.ReflectionUtils.newInstance(ReflectionUtils
	 * .java:131) when MR creates StringDouble objects "behind the scene"
	 * 
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@Test
	public void testInstantiation() throws InstantiationException, IllegalAccessException {
		StringDouble.class.newInstance();
	}

}
