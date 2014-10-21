package hadoop08.code.profession;

import static org.junit.Assert.assertEquals;
import hadoop08.code.profession.ProfessionIndexMapred.ProfessionIndexMapper;
import hadoop08.code.profession.ProfessionIndexMapred.ProfessionIndexReducer;
import hadoop08.utils.StringDouble;
import hadoop08.utils.StringDoubleList;
import hadoop08.utils.StringInteger;
import hadoop08.utils.StringIntegerList;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Calvin Wang, minwang@brandeis.edu
 */
public class ProfessionIndexMapredTest {
	MapDriver<Text, Text, Text, StringInteger> mapDriver;
	ReduceDriver<Text, StringInteger, Text, StringDoubleList> reduceDriver;
	MapReduceDriver<Text, Text, Text, StringInteger, 
						Text, StringDoubleList> mapReduceDriver;
	
	//private ProfessionIndexMapper mapper = new ProfessionIndexMapper();
	//private ProfessionIndexReducer reducer = new ProfessionIndexReducer();

	
	@Before
	public void setUp() {
		// quick and dirty -- basic logging to console instead of to file
		org.apache.log4j.BasicConfigurator.configure();
		
		// Initialize Mapper, create a mapDriver for mapper
		ProfessionIndexMapper mapper = new ProfessionIndexMapper();
		mapDriver = new MapDriver<Text, Text, Text, StringInteger>();
		mapDriver.setMapper(mapper);

		// Initialize Reducer, create a mapDriver for reducer
		ProfessionIndexReducer reducer = new ProfessionIndexReducer();
		reduceDriver = new ReduceDriver<Text, StringInteger, Text, StringDoubleList>();
		reduceDriver.setReducer(reducer);

		// Create a mapReduceDriver for mapred, for testing both together
		mapReduceDriver = new MapReduceDriver<Text, Text, Text, StringInteger, 
														Text, StringDoubleList>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() throws IOException {
	
		/*
		 * This was my original implementation -- should be the same as below
		// convert the example text to StringIntegerList first
		List<StringInteger> siList = new ArrayList<>();
		siList.add(new StringInteger("boyz",1));
		siList.add(new StringInteger("debutlp",1));
		siList.add(new StringInteger("parent",1));
		siList.add(new StringInteger("year",13));
		
		mapDriver.withInput(new Text("2 Pistols"), new StringIntegerList(siList));
		*/
		
		// create StringIntegerList from example text (github issue #22)
		//StringIntegerList siList = new StringIntegerList();
		//siList.readFromString("<boyz,1>,<debutlp,1>,<parent,1>,<year,13>");
			// removed above because current ProfessionIndexMapper does this conversion in mapper

		
		
		// feed input into the mapper
		mapDriver.withInput(new Text("2 Pistols"),
							new Text("<boyz,1>,<debutlp,1>,<parent,1>,<year,13>"));
		
		// compare to expected outputs
		mapDriver.withOutput(new Text("singer"), new StringInteger("boyz", 1));
		mapDriver.withOutput(new Text("musician"), new StringInteger("boyz", 1));
		mapDriver.withOutput(new Text("singer"), new StringInteger("debutlp", 1));
		mapDriver.withOutput(new Text("musician"), new StringInteger("debutlp", 1));
		mapDriver.withOutput(new Text("singer"), new StringInteger("parent", 1));
		mapDriver.withOutput(new Text("musician"), new StringInteger("parent", 1));
		mapDriver.withOutput(new Text("singer"), new StringInteger("year", 13));
		mapDriver.withOutput(new Text("musician"), new StringInteger("year", 13));
				
		// must get object representations instead of mapDriver.runTests()
		// because of lack of equals/hashCode method
		List<Pair<Text, StringInteger>> input = mapDriver.run();
		
		List<Pair<Text, StringInteger>> output = mapDriver.getExpectedOutputs();

		assertEquals("the input and expected output should match:\r\n" +
						input.toString() + " \r\nto\r\n " + output.toString() + "\r\n",
						0, input.toString().compareTo(output.toString()));
	}

	@Test
	public void testReducer() throws IOException {
		
		List<StringInteger> list1 = new ArrayList<StringInteger>();
		list1.add(new StringInteger("boyz", 1));
		list1.add(new StringInteger("debutlp", 1));
		list1.add(new StringInteger("parent", 1));
		list1.add(new StringInteger("year", 13));
		list1.add(new StringInteger("parent", 2));
		list1.add(new StringInteger("year", 20));
		
		// feed input into the mapper
		reduceDriver.withInput(new Text("singer"), list1);
		
		// compare to expected outputs
		
		List<StringDouble> list2 = new ArrayList<>();
		// the tiny profession_train has only 1 singer
		list2.add(new StringDouble("boyz", 1.0 / 1.0));
		list2.add(new StringDouble("debutlp", 1.0 / 1.0));
		list2.add(new StringDouble("parent", 2.0 / 1.0));
		list2.add(new StringDouble("year", 2.0 / 1.0));

		reduceDriver.withOutput(new Text("singer"), new StringDoubleList(list2));
				
		// must get object representations instead of mapDriver.runTests()
		// because of lack of equals/hashCode method
		List<Pair<Text, StringDoubleList>> input = reduceDriver.run();
		List<Pair<Text, StringDoubleList>> output = reduceDriver.getExpectedOutputs();

		assertEquals("the input and expected output should match:\r\n" + 
						input.toString() + " \r\nto\r\n " + output.toString() + "\r\n",
						0, input.toString().compareTo(output.toString()));
		//TODO: test " : " separator instead of "\t" separator
	}

	@Test
	public void mapReducer() throws IOException {
		
		// feed input into the mapper
		mapReduceDriver.withInput(new Text("2 Pistols"),
				new Text("<boyz,1>,<debutlp,1>,<parent,1>,<year,13>"));
		
		// compare to expected outputs
		
		List<StringDouble> list2 = new ArrayList<>();
		// the tiny profession_train has only 1 singer
		list2.add(new StringDouble("boyz", 1.0 / 1.0));
		list2.add(new StringDouble("debutlp", 1.0 / 1.0));
		list2.add(new StringDouble("parent", 1.0 / 1.0));
		list2.add(new StringDouble("year", 1.0 / 1.0));

		mapReduceDriver.withOutput(new Text("musician"), new StringDoubleList(list2));
		mapReduceDriver.withOutput(new Text("singer"), new StringDoubleList(list2));
				
		// must get object representations instead of mapDriver.runTests()
		// because of lack of equals/hashCode method
		List<Pair<Text, StringDoubleList>> input = mapReduceDriver.run();
		List<Pair<Text, StringDoubleList>> output = mapReduceDriver.getExpectedOutputs();

		assertEquals("the input and expected output should match:\r\n" + 
						input.toString() + " \r\nto\r\n " + output.toString() + "\r\n",
						0, input.toString().compareTo(output.toString()));
	}
	
	@Test
	public void testProfessionUtils() {
		String test = "2 Pistols : singer, musician";
		List<String> testList = new ArrayList<String>();
		testList.add(test);
		
		HashMap<String, Integer> result = ProfessionUtils.getProfessionCounts(testList);
		HashMap<String, Integer> expected = new HashMap<String, Integer>();
		expected.put("singer", 1);
		expected.put("musician", 1);
		
		assertEquals("result of getProfessionCounts should be same as expected:\r\n" +
						result.toString() + "\r\n\tshould == \r\n" + expected.toString() + "\r\n",
						expected.toString(), result.toString());
	}
	
	
	
}