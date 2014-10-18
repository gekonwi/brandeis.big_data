package code.profession;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
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
		// feed input into the mapper
		mapDriver.withInput(new Text("articleThing"), new Text("<run,5>,<star,3>,<hip,2>"));
		// compare to expected outputs
		mapDriver.withOutput(new Text("run"), new StringInteger("articleThing",5));
		mapDriver.withOutput(new Text("star"), new StringInteger("articleThing",3));
		mapDriver.withOutput(new Text("hip"), new StringInteger("articleThing",2));
		
		// must get object representations instead of mapDriver.runTests()
		// because of lack of equals/hashCode method
		List<Pair<Text, StringInteger>> input = mapDriver.run();
		List<Pair<Text, StringInteger>> output = mapDriver.getExpectedOutputs();
		
		assertEquals("the input and expected output should match",
						0, input.toString().compareTo(output.toString()));
	}
	
	
	
}
