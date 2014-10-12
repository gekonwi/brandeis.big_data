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
	private final Path testDir;

	/**
	 * 
	 * @param path
	 *            within test_data
	 */
	public TestUtils(String... path) {
		this.testDir = Paths.get("test_data", path);
	}

	public String readFile(String fileName) throws IOException {
		Path path = testDir.resolve(fileName);

		List<String> allLines = Files.readAllLines(path, Charset.forName("UTF-8"));

		StringBuilder sb = new StringBuilder();
		for (String line : allLines)
			sb.append(line + "\n");

		// delete last line break
		sb.deleteCharAt(sb.length() - 1);

		return sb.toString();
	}
}
