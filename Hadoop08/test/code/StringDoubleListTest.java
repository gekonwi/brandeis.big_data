package code;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import hadoop08.util.StringDouble;
import hadoop08.util.StringDoubleList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class StringDoubleListTest {

	@Test
	public void testEmptyConstructor() {
		StringDoubleList list = new StringDoubleList();

		// check to see if indices and indiceMap were created
		assertNotNull(list.getIndices());
		assertNotNull(list.getMap());
	}

	@Test
	public void testListConstructor() {
		List<StringDouble> arrayList = new ArrayList<>();
		arrayList.add(new StringDouble("SD1", 100.001));
		arrayList.add(new StringDouble("SD2", 200.002));
		arrayList.add(new StringDouble("SD3", 300.003));

		Map<String, Double> map = new HashMap<>();
		map.put("SD1", 100.001);
		map.put("SD2", 200.002);
		map.put("SD3", 300.003);

		StringDoubleList sdList = new StringDoubleList(arrayList);

		// check equality in list and indices
		assertEquals(arrayList, sdList.getIndices());

		// check equality in map and indiceMap
		assertEquals(map, sdList.getMap());
	}

	@Test
	public void testMapConstructor() {
		Map<String, Double> map = new HashMap<>();
		map.put("SD1", 100.001);
		map.put("SD2", 200.002);
		map.put("SD3", 300.003);

		StringDoubleList sdList = new StringDoubleList(map);

		// check equality in map and indiceMap
		assertEquals(map, sdList.getMap());

		List<StringDouble> arrayList = new ArrayList<>();
		arrayList.add(new StringDouble("SI1", 100));
		arrayList.add(new StringDouble("SI2", 200));
		arrayList.add(new StringDouble("SI3", 300));

		// traverse through all the StringDouble objects in indices and then
		// at each StringDouble object traverse through list until the
		// StringDouble object with the same String value is found, then check
		// to see if the first loops StringDouble object has the same value as
		// the second StringDouble object
		for (StringDouble si : sdList.getIndices()) {
			for (StringDouble li : arrayList) {
				if (li.getString().equals(si.getString()))
					assertTrue(si.getValue().equals(li.getValue()));
			}
		}
	}

	@Test
	public void testReadFromStringWithNoise() throws IOException {
		StringDoubleList sdList = new StringDoubleList();

		// added some noise inside the String to make sure that it is ignored
		// when reading in from String
		sdList.readFromString("<SD1,100.001>qweqwe>><SD2,200.002>   <>#123das <SD3,300.003>");

		final List<StringDouble> indices = sdList.getIndices();
		assertEquals(3, indices.size());
		assertEqualsSI(new StringDouble("SD1", 100.001), indices.get(0));
		assertEqualsSI(new StringDouble("SD2", 200.002), indices.get(1));
		assertEqualsSI(new StringDouble("SD3", 300.003), indices.get(2));
	}

	@Test
	public void testReadFromStringProper() throws IOException {
		StringDoubleList sdList = new StringDoubleList();
		sdList.readFromString("<SD1,100.001>,<SD2,200.002>,<SD3,300.003>");

		final List<StringDouble> indices = sdList.getIndices();
		assertEquals(3, indices.size());
		assertEqualsSI(new StringDouble("SD1", 100.001), indices.get(0));
		assertEqualsSI(new StringDouble("SD2", 200.002), indices.get(1));
		assertEqualsSI(new StringDouble("SD3", 300.003), indices.get(2));
	}

	@Test
	public void testToString() {
		List<StringDouble> arrayList = new ArrayList<>();
		arrayList.add(new StringDouble("SD1", 100.001));
		arrayList.add(new StringDouble("SD2", 200.002));
		arrayList.add(new StringDouble("SD3", 300.003));

		StringDoubleList sdList = new StringDoubleList(arrayList);
		assertEquals("<SD1,100.001>,<SD2,200.002>,<SD3,300.003>", sdList.toString());
	}

	@Test
	// will need implementation after figuring out how to pass in readFields
	// parameters correctly
	public void testReadFields() {
	}

	private void assertEqualsSI(StringDouble expected, StringDouble actual) {
		assertEquals(expected.getString(), actual.getString());
		assertEquals(expected.getValue(), actual.getValue());
	}

}
