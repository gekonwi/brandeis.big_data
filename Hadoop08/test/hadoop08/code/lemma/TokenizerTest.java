package hadoop08.code.lemma;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import hadoop08.TestUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

/**
 * 
 * @author Calvin Wang, minwang@brandeis.edu
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class TokenizerTest {

	private static final Path STOPWORDS_FILEPATH = Paths.get("stopwords.csv");
	private final TestUtils utils = new TestUtils(getClass());

	@Test
	public void testLemmatization() throws IOException {
		List<String> lines = Files.readAllLines(STOPWORDS_FILEPATH, Charset.forName("UTF-8"));
		HashSet<String> stopWords = new HashSet<>(lines);
		Tokenizer tokenizer = new Tokenizer(stopWords);

		String doc = "hi I am so Cool and or is we he she -this &is really |good ''cats'' {people} [gives] came.";
		List<String> lemmas = tokenizer.getLemmas(doc);

		assertEquals("hi", lemmas.get(0));

		// "I" is converted to "i" and recognized as stop word
		// "am", "so" are stop words

		assertEquals("cool", lemmas.get(1)); // everything to lower case

		// "and", "or", "is" "we" "he" "she" -> stop words
		// "-this" -> "this", "&is" -> "is" -> stop words

		assertEquals("really", lemmas.get(2));

		assertEquals("good", lemmas.get(3)); // "|", removed
		assertEquals("cat", lemmas.get(4)); // "''" removed, singular
		assertEquals("people", lemmas.get(5)); // "{", "}" removed, no singular
		assertEquals("give", lemmas.get(6)); // "[", "]" removed, infinitive
		assertEquals("come", lemmas.get(7)); // "." removed, present tense

		assertEquals(lemmas.size(), 8); // no more lemmas
	}

	@Test
	public void testRegexDoesNotEndWithOr() {
		String regex = Tokenizer.buildNoisePattern().pattern();
		assertFalse(regex.charAt(regex.length() - 1) == "|".charAt(0));
	}

	@Test
	public void testRemovesURL_HTTP() {
		String doc = "Here is some URL [http://www.url.com] which starts with http.";

		String expected = "Here is some URL which starts with http";
		String actual = Tokenizer.removeNoise(doc);

		assertEquals(expected, actual);
	}

	@Test
	public void testRemovesURL_HTTPS() {
		String doc = "Here is some URL [https://www.url.com] which starts with https.";

		String expected = "Here is some URL which starts with https";
		String actual = Tokenizer.removeNoise(doc);

		assertEquals(expected, actual);
	}

	@Test
	public void testRemovesURL_HTTPS_Without_WWW() {
		String doc = "Here is some URL [https://url.com] which starts with https but does not have a www.";

		String expected = "Here is some URL which starts with https but does not have a www";
		String actual = Tokenizer.removeNoise(doc);

		assertEquals(expected, actual);
	}

	@Test
	public void testRemovesURL_WWW() {
		String doc = "Here is some URL www.url.com without http and brackets.";

		String expected = "Here is some URL without http and brackets";
		String actual = Tokenizer.removeNoise(doc);

		assertEquals(expected, actual);
	}

	@Test
	public void testTokenize1() {
		String doc = "cool -this is# /really   |good ''bad'' {some} [one|two] dog.";
		String[] tokens = Tokenizer.removeNoise(doc).split(" ");

		assertEquals("cool", tokens[0]);
		assertEquals("this", tokens[1]); // "-" should be removed
		assertEquals("is", tokens[2]); // "#" should be removed
		assertEquals("really", tokens[3]); // "/" should be removed
		assertEquals("good", tokens[4]); // "|" and all blanks should be removed
		assertEquals("bad", tokens[5]); // "''" should be removed
		assertEquals("some", tokens[6]); // "{" and "}" should be removed
		assertEquals("one", tokens[7]); // "[" and "]" should be removed
		assertEquals("two", tokens[8]); // "|" should split
		assertEquals("dog", tokens[9]); // "." should be removed

		assertEquals(10, tokens.length); // there should be no more tokens
	}

	@Test
	public void testRemovesInfobox() throws IOException {
		String doc = utils.fileToString("Infobox");

		String cleared = Tokenizer.removeNoise(doc);

		assertEquals("First line Second line Something else", cleared);
	}

	@Test
	public void testRemovesItalic() {
		String doc = "some ''important'' message";
		doc = Tokenizer.removeNoise(doc);
		String expected = "some important message";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesBold() {
		String doc = "some ''important'' message";
		doc = Tokenizer.removeNoise(doc);
		String expected = "some important message";
		assertEquals(expected, doc);
	}

	@Test
	public void testPreservesSingleApostrophe() {
		String doc = "I'm sure we're doing well";
		doc = Tokenizer.removeNoise(doc);
		String expected = "I'm sure we're doing well";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesPicturesPreservingDescription_WithPixel() {
		String doc = "Here is ";
		doc += "[[File:Tsushima battle map-en.svg|thumb|250px|Map showing the routes of both fleets]]";
		doc += " - well.";

		doc = Tokenizer.removeNoise(doc);
		String expected = "Here is Map showing the routes of both fleets well";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesPicturesPreservingDescription_WithoutPixel() {
		String doc = "Very nice: ";
		doc += "[[File:Constitucion espanola 1978.JPG|thumb|Spanish Constitution of 1978]]";

		doc = Tokenizer.removeNoise(doc);
		String expected = "Very nice Spanish Constitution of";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesPicturesPreservingDescription_WithAlignment() {
		String doc = "Very nice: ";
		doc += "[[File:Michael Richards HS Yearbook.jpeg|thumb|left|Richards as a senior in high school, 1967.]]";

		doc = Tokenizer.removeNoise(doc);
		String expected = "Very nice Richards as a senior in high school";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesReferences() {
		String doc = "Tōgō [[Crossing the T|crossed the Russian 'T']]";
		doc += "&lt;ref&gt;Semenoff (1907) p. 70&lt;/ref&gt;";
		doc += " enabling him to fire broadsides";

		doc = Tokenizer.removeNoise(doc);
		String expected = "Tōgō Crossing the T crossed the Russian 'T' enabling him to fire broadsides";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesReferencesReluctant() {
		String doc = "Bill [[Crossing the T|crossed the Russian 'T']]";
		doc += "&lt;ref&gt;Semenoff (1907) p. 70&lt;/ref&gt;";
		doc += " enabling him to fire broadsides";
		doc += "&lt;ref&gt;Semenoff (1907) p. 70&lt;/ref&gt;";

		doc = Tokenizer.removeNoise(doc);
		String expected = "Bill Crossing the T crossed the Russian 'T' enabling him to fire broadsides";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesPosessions() {
		String doc = "my parents' house is also my sister's house";

		doc = Tokenizer.removeNoise(doc);
		String expected = "my parent house is also my sister house";
		assertEquals(expected, doc);
	}
}
