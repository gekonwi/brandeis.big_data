package code.lemma;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import util.HDFSUtils;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * @author Steven Hu, stevenhh@brandeis.edu
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class Tokenizer {

	private final StanfordCoreNLP pipeLine; // tool used for lemmatization
	private final Set<String> stopWords;

	private static final Pattern NOISE_PATTERN = buildNoisePattern();
	private static final String STOPWORDS_FILEPATH = "stopwords.csv";

	public Tokenizer() throws IOException {
		// set up the Stanford Core NLP Tool
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		pipeLine = new StanfordCoreNLP(props);

		// loading stop-words from HDFS file
		stopWords = HDFSUtils.readLines(STOPWORDS_FILEPATH);
	}

	/**
	 * Cleans noise, splits into tokens, lemmatizes each token.
	 * 
	 * @param documentText
	 * @return how often each lemma appeared in the sentence
	 */
	public List<String> getLemmas(String documentText) {
		documentText = removeNoise(documentText);

		List<String> lemmas = lemmatize(documentText);

		return filterStopWords(lemmas);
	}

	static String removeNoise(String documentText) {
		Matcher matcher = NOISE_PATTERN.matcher(documentText);
		documentText = matcher.replaceAll(" ").trim();

		// replace all multiple blanks by a single blank
		return documentText.replaceAll("\\s+", " ");
	}

	private List<String> filterStopWords(List<String> lemmas) {
		List<String> filtered = new ArrayList<>(lemmas.size());

		for (String lemma : lemmas)
			if (!stopWords.contains(lemma))
				filtered.add(lemma);

		return filtered;
	}

	static Pattern buildNoisePattern() {
		List<String> patterns = buildNoisePatternParts();

		StringBuilder sb = new StringBuilder();
		for (String pattern : patterns)
			// group each pattern and separate with OR
			sb.append("(" + pattern + ")|");

		// remove last OR
		sb.deleteCharAt(sb.length() - 1);

		// successive separators should be treated as one separation match
		String regex = "(" + sb.toString() + ")+";

		System.out.println(regex);

		// make sure the regex dot character includes line breaks
		// (e.g. for the multi-line info box)
		return Pattern.compile(regex, Pattern.DOTALL);
	}

	private static List<String> buildNoisePatternParts() {
		List<String> patterns = new ArrayList<>();

		// remove the whole InfoBox
		patterns.add("\\{\\{Infobox.*\\}\\}");

		// remove whole URLs
		patterns.add("((http(s)?:\\/\\/)|(www\\.))\\S+\\.\\S+");

		// leave only the description of a picture
		patterns.add("\\[\\[File:.+\\|thumb(\\|\\d+px)?(\\|(left|center|right))?");

		// remove whole references
		patterns.add("<ref>.+</ref>");

		// remove date and accessdate completely in any link / file
		patterns.add("\\|(access)?date=.*(\\||\\})");

		// remove attribute names like "title=", "author="
		patterns.add("\\|.*\\=");

		// remove citation prefixes (but keep title and author values etc.)
		patterns.add("\\{\\{cite web\\|url=");

		addHTMLDecodingPatterns(patterns);

		// '' for italic, ''' for bold, but preserve the single '
		patterns.add("''+");

		// remove 's and s' suffixes indicating either possession or "is" or
		// "has" - all three cases are irrelevant for indexing
		patterns.add("('s|s')\\s");

		addUnwantedCharPatterns(patterns);

		return patterns;
	}

	private static void addUnwantedCharPatterns(List<String> patterns) {
		// Unwanted characters, separated by a blank.
		String chars = "\" ` ´ . , : ; ! ? ( ) [ ] { } < > = / | \\ % & # § $ _ - ~ * ° ^ +";
		chars += " s d"; // white space (s) and digits (s)

		for (String c : chars.split(" "))
			// escape to avoid mis-interpretation as a special regex character
			patterns.add("\\" + c);
	}

	private static void addHTMLDecodingPatterns(List<String> patterns) {
		// TODO are these needed? if HTML is decoded while XML parsing, then
		// it's not.
		patterns.add("&lt"); // "<" in HTML encoding
		patterns.add("&gt"); // ">" in HTML encoding
		patterns.add("&amp"); // "&" in HTML encoding
		patterns.add("&quot"); // " (quotation mark) in HTML encoding
	}

	/**
	 * Lemmatizes each element. Inspiration from:
	 * http://stackoverflow.com/questions/1578062/lemmatization-java
	 * 
	 * @param documentText
	 * @return
	 */
	public List<String> lemmatize(String documentText) {
		List<String> lemmas = new ArrayList<>();

		Annotation document = new Annotation(documentText);
		this.pipeLine.annotate(document);

		List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		for (CoreMap sentence : sentences) {
			for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
				String lemma = token.get(LemmaAnnotation.class).toLowerCase();
				lemmas.add(lemma);
			}
		}

		return lemmas;
	}
}