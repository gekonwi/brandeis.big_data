package code.lemma;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.stream.XMLStreamException;

import org.junit.Test;

import code.TestUtils;

/**
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class LemmaIndexMapredTest {

	TestUtils utils = new TestUtils(getClass());

	@Test
	public void testGetArticle() throws IOException, XMLStreamException {
		Path testDir = Paths.get("test_data", "LemmaIndexMapredTest");
		for (String fileName : testDir.toFile().list())
			if (!fileName.endsWith(" - text"))
				testArticleContent(fileName);

	}

	private void testArticleContent(String fileName) throws IOException, XMLStreamException {
		String xml = utils.fileToString(fileName);

		String expected = utils.fileToString(fileName + " - text");

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
}
