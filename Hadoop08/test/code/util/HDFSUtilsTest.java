package code.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import util.HDFSUtils;
import code.TestUtils;

/**
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 * 
 */
public class HDFSUtilsTest {
	TestUtils utils = new TestUtils(getClass());

	@Test
	public void getProfessionCountsPerformanceTest() throws IOException {
		final long ACCEPTED_TOTAL_MILLIS = 5_000;

		System.out.println("starting reading in");
		long millisAtStart = System.currentTimeMillis();

		Path professionTrainPath = utils.getInputFilePath("profession_train.txt");
		List<String> fileLines = Files.readAllLines(professionTrainPath, Charset.forName("UTF-8"));
		assertEquals(673_988, fileLines.size());

		long millisReading = System.currentTimeMillis() - millisAtStart;
		System.out.println("finished reading in after " + millisReading + " millis");

		System.out.println("starting parsing");
		long millisBeforeParsing = System.currentTimeMillis();
		HDFSUtils.getProfessionCounts(fileLines);

		long millisParsing = System.currentTimeMillis() - millisBeforeParsing;
		System.out.println("finished parsing after " + millisParsing + " millis");

		long millisTotal = millisReading + millisParsing;
		System.out.println("finished reading + parsing after " + millisTotal + " millis");

		assertTrue(millisTotal < ACCEPTED_TOTAL_MILLIS);
	}

	@Test
	public void getProfessionCountsTest() throws IOException {
		Path path = utils.getInputFilePath("getProfessionCountsTest_input.txt");
		List<String> professions = Files.readAllLines(path, Charset.forName("UTF-8"));

		Map<String, Integer> counts = HDFSUtils.getProfessionCounts(professions);
		assertEquals("there are 8 unique professions in the test set", 8, counts.size());

		assertEquals(1, (int) counts.get("rugby union player"));
		assertEquals(6, (int) counts.get("footballer"));
		assertEquals(1, (int) counts.get("historian"));
		assertEquals(1, (int) counts.get("legal scholar"));
		assertEquals(1, (int) counts.get("social scientist"));
		assertEquals(1, (int) counts.get("biographer"));
		assertEquals(1, (int) counts.get("footballer2"));
		assertEquals(1, (int) counts.get("basketball player"));
	}
}
