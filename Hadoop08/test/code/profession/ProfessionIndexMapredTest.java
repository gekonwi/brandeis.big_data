package code.profession;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import code.profession.ProfessionIndexMapred.ProfessionIndexMapper;
import code.profession.ProfessionIndexMapred.ProfessionIndexReducer;
import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import util.StringDoubleList;
import util.StringDoubleList.StringDouble;

/**
 * 
 * @author Calvin Wang, minwang@brandeis.edu
 */
public class ProfessionIndexMapredTest {
	MapDriver<Text, StringIntegerList, Text, StringInteger> mapDriver;
	ReduceDriver<Text, StringInteger, Text, StringDoubleList> reduceDriver;
	MapReduceDriver<Text, StringIntegerList, Text, StringInteger, 
						Text, StringDoubleList> mapReduceDriver;
	
	private ProfessionIndexMapper mapper = new ProfessionIndexMapper();
	private ProfessionIndexReducer reducer = new ProfessionIndexReducer();

	
	@Before
	public void setUp() {
		// quick and dirty -- basic logging to console instead of to file
		org.apache.log4j.BasicConfigurator.configure();
		
		// Initialize Mapper, create a mapDriver for mapper
		ProfessionIndexMapper mapper = new ProfessionIndexMapper();
		mapDriver = new MapDriver<Text, StringIntegerList, Text, StringInteger>();
		mapDriver.setMapper(mapper);

		// Initialize Reducer, create a mapDriver for reducer
		ProfessionIndexReducer reducer = new ProfessionIndexReducer();
		reduceDriver = new ReduceDriver<Text, StringInteger, Text, StringDoubleList>();
		reduceDriver.setReducer(reducer);

		// Create a mapReduceDriver for mapred, for testing both together
		mapReduceDriver = new MapReduceDriver<Text, StringIntegerList, Text, StringInteger, 
														Text, StringIntegerList>();
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
		StringIntegerList siList = new StringIntegerList();
		siList.readFromString("<boyz,1>,<debutlp,1>,<parent,1>,<year,13>");

		// feed input into the mapper
		mapDriver.withInput(new Text("2 Pistols"), siList);
		
		// compare to expected outputs
		mapDriver.withOutput(new Text("singer"), new StringInteger("boyz", 1));
		mapDriver.withOutput(new Text("singer"), new StringInteger("debutlp", 1));
		mapDriver.withOutput(new Text("singer"), new StringInteger("parent", 1));
		mapDriver.withOutput(new Text("singer"), new StringInteger("year", 13));
		mapDriver.withOutput(new Text("musician"), new StringInteger("boyz", 1));
		mapDriver.withOutput(new Text("musician"), new StringInteger("debutlp", 1));
		mapDriver.withOutput(new Text("musician"), new StringInteger("parent", 1));
		mapDriver.withOutput(new Text("musician"), new StringInteger("year", 13));
		
		// must get object representations instead of mapDriver.runTests()
		// because of lack of equals/hashCode method
		List<Pair<Text, StringInteger>> input = mapDriver.run();
		List<Pair<Text, StringInteger>> output = mapDriver.getExpectedOutputs();
		
		assertEquals("the input and expected output should match",
						0, input.toString().compareTo(output.toString()));
	}
	
	
	
}
