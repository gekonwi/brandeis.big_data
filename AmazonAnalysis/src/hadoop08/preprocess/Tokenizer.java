package hadoop08.preprocess;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	public static final Pattern NOISE_PATTERN = buildNoisePattern();
	public static final Pattern LINK_PATTERN = Pattern
			.compile("\\[\\[([^\\]]*\\|)*(?<shownPart>.*?)\\]\\]");
	public static final Pattern CITATION_PATTERN = Pattern.compile("\\{\\{[^\\{\\}]*\\}\\}");

	public Tokenizer(HashSet<String> stopWords) {
		// set up the Stanford Core NLP Tool
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		pipeLine = new StanfordCoreNLP(props);

		this.stopWords = stopWords;
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

	static String removeNoise(String doc) {
		doc = removeLinkKeepShownPart(doc.toLowerCase());

		doc = removeWhileChanges(doc, CITATION_PATTERN);
		doc = removeWhileChanges(doc, NOISE_PATTERN);

		doc = doc.trim();

		// replace all multiple blanks by a single blank
		return doc.replaceAll("\\s+", " ");
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
		// String regex = "(" + sb.toString() + ")+?";
		String regex = sb.toString();

		System.out.println(regex);

		// make sure the regex dot character includes line breaks
		// (e.g. for the multi-line info box)
		return Pattern.compile(regex, Pattern.DOTALL);
	}

	static String removeLinkKeepShownPart(String doc) {
		Matcher m = LINK_PATTERN.matcher(doc);
		return m.replaceAll(" ${shownPart} ");
	}

	/**
	 * Apply the given pattern on the given document until it does not change
	 * anymore.
	 * 
	 * @param doc
	 *            text potentially containing pattern matches
	 * @param pat
	 *            the pattern to be applied (potentially multiple times)
	 * @return <code>doc</code> text with all {{x}} elements replaced by blanks
	 */
	private static String removeWhileChanges(String doc, Pattern pat) {
		String oldDoc = "";
		while (!doc.equals(oldDoc)) {
			oldDoc = doc;
			Matcher m = pat.matcher(doc);
			doc = m.replaceAll(" ");
		}
		return doc;
	}

	private static List<String> buildNoisePatternParts() {
		List<String> patterns = new ArrayList<>();

		// remove whole URLs
		patterns.add("((https?://www\\.)|(https?://)|(www\\.))\\w+\\.[a-z]+(/\\w+)*");

		// remove whole references
		patterns.add("&lt;ref&gt;.+?&lt;/ref&gt;");

		// '' for italic, ''' for bold, but preserve the single '
		patterns.add("''+");

		// remove 's and s' suffixes indicating either possession or "is" or
		// "has" - all three cases are irrelevant for indexing
		patterns.add("('s|s')\\s");

		// remove HTML encoding
		patterns.add("&[a-z]+?;");

		patterns.addAll(getUnwantedCharPatterns());

		return patterns;
	}

	private static List<String> getUnwantedCharPatterns() {
		/*
		 * we can't use a simple [^a-zA-Z] here because we need to preserve
		 * letter variations like in the name Tōgō
		 */

		// Unwanted characters, separated by a blank.
		String chars = "\" ` ´ . , : ; ! ? ( ) [ ] { } < > = / | \\ % & # § $ _ - ~ * ° ^ +";
		chars += " s d"; // white space (s) and digits (s)

		List<String> patterns = new ArrayList<>();
		for (String c : chars.split(" "))
			// escape to avoid mis-interpretation as a special regex character
			patterns.add("\\" + c);

		return patterns;
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