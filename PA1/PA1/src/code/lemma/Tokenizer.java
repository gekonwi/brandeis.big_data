package code.lemma;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class Tokenizer {

	private final StanfordCoreNLP pipeline; // tool used for lemmatization
	private final Set<String> stopWords;

	public Tokenizer() throws FileNotFoundException {
		// set up the Stanford Core NLP Tool
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, pos, lemma");
		pipeline = new StanfordCoreNLP(props);

		// loading stop-words from file
		stopWords = new HashSet<String>();
		Scanner in = new Scanner(new FileReader("stopwords.txt"));
		while (in.hasNext())
			stopWords.add(in.next());
		in.close();
	}

	/**
	 * Cleans noise, splits into tokens, lemmatizes each token.
	 * 
	 * @param documentText
	 * @return how often each lemma appeared in the sentence
	 */
	public List<String> getLemmas(String documentText) {
		documentText = removeURLs(documentText);

		String[] tokens = tokenize(documentText);

		List<String> lemmas = lemmatize(tokens);

		return filterLemmas(lemmas);
	}

	public static String[] tokenize(String documentText) {
		/*
		 * splitting using regex. each unwanted character like white space or *,
		 * #, ? or sequence of unwanted characters is used as separator between
		 * tokens. This removes the separators and extracts tokens before and
		 * after each separator.
		 */
		return documentText.split(getSeparatorRegex());
	}

	public static String removeURLs(String documentText) {
		String[] tokens = documentText.split("\\s+");

		StringBuilder sb = new StringBuilder();
		for (String token : tokens)
			if (!token.matches(".*((http(s)?:\\/\\/)|www).*"))
				sb.append(token + " ");

		// remove last blank
		sb.deleteCharAt(sb.length() - 1);

		return sb.toString();
	}

	private List<String> filterLemmas(List<String> lemmas) {
		List<String> filtered = new ArrayList<>(lemmas.size());

		for (String lemma : lemmas)
			if (!stopWords.contains(lemma))
				filtered.add(lemma);

		return filtered;
	}

	public static String getSeparatorRegex() {
		/*
		 * Unwanted characters, separated by a blank. Used as separators between
		 * tokens. The character " as well as \ has to be escaped using \ in
		 * order to be part of the string.
		 */
		String sep = "\" ' ` ´ . , : ; ! ? ( ) [ ] { } < > = / | \\ % & # § $ _ - ~ * ° ^ +";
		sep += " s d"; // white space (s) and digits (s)

		StringBuilder sb = new StringBuilder();
		for (String s : sep.split(" ")) {
			// escape to avoid mis-interpretation as a special regex character
			sb.append("\\" + s);

			// "OR"
			sb.append("|");
		}

		// TODO are these needed? if HTML is decoded while reading in, it's not.
		sb.append("&lt|"); // "<" in HTML encoding
		sb.append("&gt|"); // ">" in HTML encoding
		sb.append("&amp"); // "&" in HTML encoding

		// successive separators should be treated as one separation match
		return "(" + sb.toString() + ")+";
	}

	/**
	 * Lemmatizes each element. Inspiration from:
	 * http://stackoverflow.com/questions/1578062/lemmatization-java
	 * 
	 * @param tokens
	 * @return
	 */
	public List<String> lemmatize(String[] tokens) {
		List<String> lemmas = new ArrayList<>();

		// let Stanford-NLP tokenize it the way it likes
		StringBuilder sb = new StringBuilder();
		for (String token : tokens)
			sb.append(token + " ");

		Annotation document = new Annotation(sb.toString());
		this.pipeline.annotate(document);

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