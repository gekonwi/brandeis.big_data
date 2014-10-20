package hadoop08.code.inverted;

import static org.junit.Assert.*;
import hadoop08.code.inverted.InvertedIndexMapred.InvertedIndexMapper;
import hadoop08.code.inverted.InvertedIndexMapred.InvertedIndexReducer;
import hadoop08.util.StringInteger;
import hadoop08.util.StringIntegerList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.apache.hadoop.mrunit.types.Pair;

import static org.mockito.Mockito.*;

import org.junit.*;

public class InvertedIndexMapredTest {
/*
	@Test
	public void testMapper() {
		InvertedIndexMapper mapper = new InvertedIndexMapper();
		// TODO to be continued - I need a fake Context object,
		// using mocito framework?
	}

}*/

	MapDriver<Text, Text, Text, StringInteger> mapDriver;
	ReduceDriver<Text, StringInteger, Text, StringIntegerList> reduceDriver;
	MapReduceDriver<Text, Text, Text, StringInteger, 
						Text, StringIntegerList> mapReduceDriver;
	
	@Before
	public void setUp() {
		// quick and dirty -- basic logging to console instead of to file
		org.apache.log4j.BasicConfigurator.configure();
		
		Mapper<Text, Text, Text, StringInteger>.Context context = mock(Context.class);
		
		// Initialize Mapper, create a mapDriver for mapper
		InvertedIndexMapper mapper = new InvertedIndexMapper();
		mapDriver = new MapDriver<Text, Text, Text, StringInteger>();
		mapDriver.setMapper(mapper);

		// Initialize Reducer, create a mapDriver for reducer
		InvertedIndexReducer reducer = new InvertedIndexReducer();
		reduceDriver = new ReduceDriver<Text, StringInteger, Text, StringIntegerList>();
		reduceDriver.setReducer(reducer);

		// Create a mapReduceDriver for mapred, for testing both together
		mapReduceDriver = new MapReduceDriver<Text, Text, Text, StringInteger, 
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
	
	@Test
	public void testReducer() throws IOException {
		
		/*
		 * transform:
		 * 
		 * lemma1 <article_id1,freq1>
		 * 
		 * lemma1 <article_id2,freq2>
		 * in
		 * lemma2 <article_id1,freq3>
		 * 
		 * into:
		 * 
		 * lemma1 <article_id1,freq1>,<article_id2,freq2>
		 * 
		 * lemma2 <article_id1,freq3>
		 */
		
		List<StringInteger> siList = new ArrayList<>();
		siList.add(new StringInteger("article_id1",5));
		siList.add(new StringInteger("article_id2",10));
		// feed input into the reducer
		reduceDriver.withInput(new Text("lemma1"), siList);
		// compare to expected outputs
		reduceDriver.withOutput(new Text("lemma1"), new StringIntegerList(siList));
		
		List<Pair<Text, StringIntegerList>> input = reduceDriver.run();
		List<Pair<Text, StringIntegerList>> output = reduceDriver.getExpectedOutputs();
		
		assertEquals("the input and expected output should match", 
						0, input.toString().compareTo(output.toString()));
	}
	
}
