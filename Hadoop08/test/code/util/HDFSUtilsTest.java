package code.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
}
