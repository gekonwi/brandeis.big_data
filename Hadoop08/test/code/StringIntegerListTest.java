package code;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import util.StringInteger;
import util.StringIntegerList;

public class StringIntegerListTest {

	StringIntegerList sl_1;
	StringIntegerList sl_2;
	StringIntegerList sl_3;

	List<StringInteger> list;
	Map<String, Integer> map;

	@Test
	public void testEmptyConstructor() {
		sl_1 = new StringIntegerList();

		// check to see if indices and indiceMap were created
		assertNotNull(sl_1.getIndices());
		assertNotNull(sl_1.getMap());
	}

	@Test
	public void testListConstructor() {
		list = new ArrayList<>();
		list.add(new StringInteger("SI1", 100));
		list.add(new StringInteger("SI2", 200));
		list.add(new StringInteger("SI3", 300));

		map = new HashMap<>();
		map.put("SI1", 100);
		map.put("SI2", 200);
		map.put("SI3", 300);

		sl_2 = new StringIntegerList(list);

		// check equality in list and indices
		assertEquals(list, sl_2.getIndices());

		// check equality in map and indiceMap
		assertEquals(map, sl_2.getMap());
	}

	@Test
	public void testMapConstructor() {
		map = new HashMap<>();
		map.put("SI1", 100);
		map.put("SI2", 200);
		map.put("SI3", 300);

		sl_3 = new StringIntegerList(map);

		// check equality in map and indiceMap
		assertEquals(map, sl_3.getMap());

		list = new ArrayList<>();
		list.add(new StringInteger("SI1", 100));
		list.add(new StringInteger("SI2", 200));
		list.add(new StringInteger("SI3", 300));

		// traverse through all the StringInteger objects in indices and then
		// at each StringInteger object traverse through list until the
		// StringInteger object with the same String value is found, then check
		// to see if the first loops StringInteger object has the same value as
		// the second StringInteger object
		for (StringInteger si : sl_3.getIndices()) {
			for (StringInteger li : list) {
				if (li.getString().equals(si.getString()))
					assertTrue(si.getValue().equals(li.getValue()));
			}
		}
	}

	@Test
	public void testReadFromStringAndToString() throws IOException {
		sl_1 = new StringIntegerList();

		// added some noise inside the String to make sure that it is ignored
		// when reading in from String
		sl_1.readFromString("<SI1,100>qweqwe>><SI2,200>   <>#123das <SI3,300>");

		// testing both readFromString (whether parsing is correct) and toString
		// (whether it is returning the String in the correct format)
		assertEquals("<SI1,100>,<SI2,200>,<SI3,300>", sl_1.toString());
	}

	@Test
	// will need implementation after figuring out how to pass in readFields
	// parameters correctly
	public void testReadFields() {
	}

}
