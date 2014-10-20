package code;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 * 
 */
public class TestUtils {
	private final Path testInputDir;
	public static final String TEST_DATA_DIR_NAME = "test_data";

	/**
	 * Create a {@link TestUtils} instance for the provided testing class. The
	 * test data is expected to be in the directory
	 * <code>{@value #TEST_DATA_DIR_NAME} + "/" + testingClass.getSinpleName()</code>
	 * .
	 * <p>
	 * Example: the if <code>testingClass == TokenizerTest.class</code> the
	 * corresponding test files are expected to be found in:<br>
	 * <code>{@value #TEST_DATA_DIR_NAME} + "/TokenizerTest"</code>
	 * 
	 * @param testingClass
	 *            the class which intends to use this instance to read in its
	 *            test input files
	 */
	public TestUtils(Class<?> testingClass) {
		this.testInputDir = Paths.get(TEST_DATA_DIR_NAME, testingClass.getSimpleName());
	}

	public Path getInputFilePath(String fileName) {
		return testInputDir.resolve(fileName);
	}

	public Path getInputDir() {
		return testInputDir;
	}

	/**
	 * Reads in he whole file and returns it as a single String. All line breaks
	 * are preserved with \n characters.
	 * 
	 * @param fileName
	 *            the name of the file to be read in. This file has to be inside
	 *            the test_data/TestClass directory.
	 * @return
	 * @throws IOException
	 */
	public String fileToString(String fileName) throws IOException {
		Path path = testInputDir.resolve(fileName);

		List<String> allLines = Files.readAllLines(path, Charset.forName("UTF-8"));

		StringBuilder sb = new StringBuilder();
		for (String line : allLines)
			sb.append(line + "\n");

		// delete last line break
		sb.deleteCharAt(sb.length() - 1);

		return sb.toString();
	}
}
