package code.lemma;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

import edu.stanford.nlp.ling.CoreAnnotations.*;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.util.CoreMap;

/**
 * @author Hadoop 08 (Steven, Calvin, Paul, Georg)
 * @version 0.3
 * @since 10/2/14
 */
public class Tokenizer{

	private StanfordCoreNLP pipeline; //tool used for lemmatization
	private List<String> stopWords;
	
	public Tokenizer() throws FileNotFoundException{
        //set up and initialize the Stanford Core NLP Tool
		Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
        pipeline = new StanfordCoreNLP(props);
        
        //loading stop-words into an ArrayList
        stopWords = new ArrayList<String>();
        Scanner in = new Scanner(new FileReader("stopwords.txt"));
	        while(in.hasNext())
	        	stopWords.add(in.next());
	        in.close();
	}

	/**
	 * Does the work of splitting an input sentence into a List<String>, cleaning up unnecessary noise, lemmatizing each element in the 
	 * sentence, and filter out words part of the stop-words list
	 * @param sentence
	 * @return
	 */
	public Map<String, Integer> tokenize(String sentence){
		List<String> words = new ArrayList<String>();
		//splitting using regex
		//this implementation *can* be consolidated into method lemmatize(s), but could be messy with italics ('') and bolds (''')
		words = Arrays.asList(sentence.split("\\s+|('')|(''')|(``)|[!.?:;,{}|=<>/*%&#$_`'~+?????]|-|\\[|\\]"));
		//kinda weird, but this basically turns the current words ArrayList back into a String... sort've
		sentence = Arrays.toString(words.toArray());
		
		return lemmatize(sentence);
	}
	
	/**
	 * Helper method that splits an input String into a List<String> and lemmatizes each element during the adding process.
	 * Inspiration from: http://stackoverflow.com/questions/1578062/lemmatization-java
	 * @param documentText
	 * @return
	 */
	public Map<String, Integer> lemmatize(String documentText){
		Map<String, Integer> lemmas = new HashMap<String, Integer>();
        Annotation document = new Annotation(documentText);
        this.pipeline.annotate(document);

        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for(CoreMap sentence: sentences) {
            for(CoreLabel token: sentence.get(TokensAnnotation.class)){
            	String lem = token.get(LemmaAnnotation.class).toLowerCase();
            	//if statement is doing a lot of magic--taking out unnecessary noise
            	if(!lem.matches(".*\\d.*") && !lem.matches(".*&lt.*") && !lem.matches(".*&gt.*") && !lem.matches(".*&amp.*") 
            		&& !lem.matches(".*http.*") && !lem.matches("[,.!?:;{}]") && !lem.equals("-lsb-") && !lem.equals("-rsb-")
            		&& !lem.equals("-lrb-") && !lem.matches(".*/.*") && !stopWords.contains(lem))
            		if(lemmas.get(lem) != null)
            			lemmas.put(lem, lemmas.get(lem)+1);
            		else
            			lemmas.put(lem, 1);
            }
        }
        return lemmas;
    }
}