package code.lemma;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.junit.Test;

/**
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class LemmaIndexMapredTest {

	@Test
	public void testGetArticle() throws IOException, XMLStreamException {
		Path testDir = Paths.get("test_data", "LemmaIndexMapredTest");
		for (String fileName : testDir.toFile().list())
			if (!fileName.endsWith(" - text"))
				testArticleContent(fileName);

	}

	private void testArticleContent(String fileName) throws IOException, XMLStreamException {
		String xml = readFile(fileName);

		String expected = readFile(fileName + " - text");

		String actual;
		try {
			actual = LemmaIndexMapred.LemmaIndexMapper.getArticleBody(xml);
		} catch (XMLStreamException e) {
			System.err.println("Error while parsing [" + fileName + "]");
			e.printStackTrace();
			throw e;
		}

		assertEquals(fileName, expected, actual);
	}

	private String readFile(String fileName) throws IOException {
		Path path = Paths.get("test_data", "LemmaIndexMapredTest", fileName);

		List<String> allLines = Files.readAllLines(path, Charset.forName("UTF-8"));

		StringBuilder sb = new StringBuilder();
		for (String line : allLines)
			sb.append(line + "\n");

		sb.delete(sb.lastIndexOf("\n"), sb.length());

		return sb.toString();
	}
}
