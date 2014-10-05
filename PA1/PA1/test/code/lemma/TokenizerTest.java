package code.lemma;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import code.TestUtils;

/**
 * 
 * @author Calvin Wang, minwang@brandeis.edu
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class TokenizerTest {

	TestUtils utils = new TestUtils("TokenizerTest");

	@Test
	public void testGetLemmas() throws FileNotFoundException {
		Tokenizer tocenizer = new Tokenizer();

		String doc = "hi I am so Cool and or is we he she -this &is really |good ''cats'' {people} [gives] came.";
		List<String> lemmas = tocenizer.getLemmas(doc);

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
		System.out.println(regex);
		assertFalse(regex.charAt(regex.length() - 1) == "|".charAt(0));
	}

	@Test
	public void testRemoveURL_HTTP() {
		String doc = "Here is some URL [http://www.url.com] which starts with http.";

		String expected = "Here is some URL which starts with http";
		String actual = Tokenizer.removeNoise(doc);

		assertEquals(expected, actual);
	}

	@Test
	public void testRemoveURL_HTTPS() {
		String doc = "Here is some URL [https://www.url.com] which starts with https.";

		String expected = "Here is some URL which starts with https";
		String actual = Tokenizer.removeNoise(doc);

		assertEquals(expected, actual);
	}

	@Test
	public void testRemoveURL_HTTPS_Without_WWW() {
		String doc = "Here is some URL [https://url.com] which starts with https but does not have a www.";

		String expected = "Here is some URL which starts with https but does not have a www";
		String actual = Tokenizer.removeNoise(doc);

		assertEquals(expected, actual);
	}

	@Test
	public void testRemoveURL_WWW() {
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
	public void testTokenizeRemovesInfobox() throws IOException {
		String doc = utils.readFile("Infobox");

		String cleared = Tokenizer.removeNoise(doc);

		assertEquals("First line Second line", cleared);
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
	public void testRemovesPicturesPreservingDescription() {
		String doc = "Here is ";
		doc += "[[File:Tsushima battle map-en.svg|thumb|250px|Map showing the routes of both fleets]]";
		doc += " - well.";

		doc = Tokenizer.removeNoise(doc);
		String expected = "Here is Map showing the routes of both fleets well";
		assertEquals(expected, doc);
	}

	@Test
	public void testRemovesReferences() {
		String doc = "Tōgō  [[Crossing the T|crossed the Russian 'T']]";
		doc += "<ref>Semenoff (1907) p. 70</ref>";
		doc += " enabling him to fire broadsides";

		doc = Tokenizer.removeNoise(doc);
		String expected = "Tōgō Crossing the T crossed the Russian 'T' enabling him to fire broadsides";
		assertEquals(expected, doc);
	}
}
