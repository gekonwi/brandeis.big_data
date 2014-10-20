package code;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import hadoop08.util.StringInteger;
import hadoop08.util.StringIntegerList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class StringIntegerListTest {

	@Test
	public void testEmptyConstructor() {
		StringIntegerList list = new StringIntegerList();

		// check to see if indices and indiceMap were created
		assertNotNull(list.getIndices());
		assertNotNull(list.getMap());
	}

	@Test
	public void testListConstructor() {
		List<StringInteger> arrayList = new ArrayList<>();
		arrayList.add(new StringInteger("SI1", 100));
		arrayList.add(new StringInteger("SI2", 200));
		arrayList.add(new StringInteger("SI3", 300));

		Map<String, Integer> map = new HashMap<>();
		map.put("SI1", 100);
		map.put("SI2", 200);
		map.put("SI3", 300);

		StringIntegerList siList = new StringIntegerList(arrayList);

		// check equality in list and indices
		assertEquals(arrayList, siList.getIndices());

		// check equality in map and indiceMap
		assertEquals(map, siList.getMap());
	}

	@Test
	public void testMapConstructor() {
		Map<String, Integer> map = new HashMap<>();
		map.put("SI1", 100);
		map.put("SI2", 200);
		map.put("SI3", 300);

		StringIntegerList siList = new StringIntegerList(map);

		// check equality in map and indiceMap
		assertEquals(map, siList.getMap());

		List<StringInteger> arrayList = new ArrayList<>();
		arrayList.add(new StringInteger("SI1", 100));
		arrayList.add(new StringInteger("SI2", 200));
		arrayList.add(new StringInteger("SI3", 300));

		// traverse through all the StringInteger objects in indices and then
		// at each StringInteger object traverse through list until the
		// StringInteger object with the same String value is found, then check
		// to see if the first loops StringInteger object has the same value as
		// the second StringInteger object
		for (StringInteger si : siList.getIndices()) {
			for (StringInteger li : arrayList) {
				if (li.getString().equals(si.getString()))
					assertTrue(si.getValue().equals(li.getValue()));
			}
		}
	}

	@Test
	public void testReadFromStringWithNoise() throws IOException {
		StringIntegerList siList = new StringIntegerList();

		// added some noise inside the String to make sure that it is ignored
		// when reading in from String
		siList.readFromString("<SI1,100>qweqwe>><SI2,200>   <>#123das <SI3,300>");

		final List<StringInteger> indices = siList.getIndices();
		assertEquals(3, indices.size());
		assertEqualsSI(new StringInteger("SI1", 100), indices.get(0));
		assertEqualsSI(new StringInteger("SI2", 200), indices.get(1));
		assertEqualsSI(new StringInteger("SI3", 300), indices.get(2));
	}

	@Test
	public void testReadFromStringProper() throws IOException {
		StringIntegerList siList = new StringIntegerList();
		siList.readFromString("<SI1,100>,<SI2,200>,<SI3,300>");

		final List<StringInteger> indices = siList.getIndices();
		assertEquals(3, indices.size());
		assertEqualsSI(new StringInteger("SI1", 100), indices.get(0));
		assertEqualsSI(new StringInteger("SI2", 200), indices.get(1));
		assertEqualsSI(new StringInteger("SI3", 300), indices.get(2));
	}

	@Test
	public void testToString() {
		List<StringInteger> arrayList = new ArrayList<>();
		arrayList.add(new StringInteger("SI1", 100));
		arrayList.add(new StringInteger("SI2", 200));
		arrayList.add(new StringInteger("SI3", 300));

		StringIntegerList siList = new StringIntegerList(arrayList);
		assertEquals("<SI1,100>,<SI2,200>,<SI3,300>", siList.toString());
	}

	@Test
	// will need implementation after figuring out how to pass in readFields
	// parameters correctly
	public void testReadFields() {
	}

	private void assertEqualsSI(StringInteger expected, StringInteger actual) {
		assertEquals(expected.getString(), actual.getString());
		assertEquals(expected.getValue(), actual.getValue());
	}

}
