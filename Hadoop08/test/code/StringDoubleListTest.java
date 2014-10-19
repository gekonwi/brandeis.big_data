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

import util.StringDouble;
import util.StringDoubleList;

public class StringDoubleListTest {

	StringDoubleList sd_1;
	StringDoubleList sd_2;
	StringDoubleList sd_3;

	List<StringDouble> list;
	Map<String, Double> map;

	@Test
	public void testEmptyConstructor() {
		sd_1 = new StringDoubleList();

		// check to see if indices and indiceMap were created
		assertNotNull(sd_1.getIndices());
		assertNotNull(sd_1.getMap());
	}

	@Test
	public void testListConstructor() {
		list = new ArrayList<>();
		list.add(new StringDouble("SD1", 100.001));
		list.add(new StringDouble("SD2", 200.002));
		list.add(new StringDouble("SD3", 300.003));

		map = new HashMap<>();
		map.put("SD1", 100.001);
		map.put("SD2", 200.002);
		map.put("SD3", 300.003);

		sd_2 = new StringDoubleList(list);

		// check equality in list and indices
		assertEquals(list, sd_2.getIndices());

		// check equality in map and indiceMap
		assertEquals(map, sd_2.getMap());
	}

	@Test
	public void testMapConstructor() {
		map = new HashMap<>();
		map.put("SD1", 100.001);
		map.put("SD2", 200.002);
		map.put("SD3", 300.003);

		sd_3 = new StringDoubleList(map);

		// check equality in map and indiceMap
		assertEquals(map, sd_3.getMap());

		list = new ArrayList<>();
		list.add(new StringDouble("SD1", 100.001));
		list.add(new StringDouble("SD2", 200.002));
		list.add(new StringDouble("SD3", 300.003));

		// traverse through all the StringDouble objects in indices and then
		// at each StringDouble object traverse through list until the
		// StringDouble object with the same String value is found, then check
		// to see if the first loops StringDouble object has the same value as
		// the second StringDouble object
		for (StringDouble sd : sd_3.getIndices()) {
			for (StringDouble ld : list) {
				if (ld.getString().equals(sd.getString()))
					assertTrue(sd.getValue().equals(ld.getValue()));
			}
		}
	}

	@Test
	public void testReadFromStringAndToString() throws IOException {
		sd_1 = new StringDoubleList();

		// added some noise inside the String to make sure that it is ignored
		// when reading in from String
		sd_1.readFromString("<SD1,100.001>qweqwe>><SD2,200.002>   <>#123das <SD3,300.003>");

		// testing both readFromString (whether parsing is correct) and toString
		// (whether it is returning the String in the correct format)
		assertEquals("<SD1,100.001>,<SD2,200.002>,<SD3,300.003>", sd_1.toString());
	}

	@Test
	// will need implementation after figuring out how to pass in readFields
	// parameters correctly
	public void testReadFields() {
	}

}
