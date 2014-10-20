package hadoop08.code.lemma;

import static org.junit.Assert.assertEquals;
import hadoop08.TestUtils;
import hadoop08.code.lemma.LemmaIndexMapred;

import java.io.IOException;
import java.nio.file.Path;

import javax.xml.stream.XMLStreamException;

import org.junit.Test;

/**
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class LemmaIndexMapredTest {

	TestUtils utils = new TestUtils(getClass());

	@Test
	public void testGetArticle() throws IOException, XMLStreamException {
		Path testDir = new TestUtils(getClass()).getInputDir();
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
