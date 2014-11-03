package hadoop08.code.preprocess;

import hadoop08.code.preprocess.Tokenizer;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Preprocesses the original input file, by using the Tokenizer we developed from
 * assignment 1 and 2 to remove stopwords and punctuation and to lemmatize the rest.
 * 
 * @author Calvin Wang
 *
 */
public class PreProcess {
	
	private static final String STOPWORDS_FILEPATH = "stopwords.csv";
	private static HashSet<String> stopWords;
	private static Tokenizer tokenizer;
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		if (args.length != 2)
			throw new IllegalArgumentException(
					"need two parameters: inputPath and outputPath");
		
		File inputPath = new File(args[0]);
		File outputPath = new File(args[0]);
		
		setupStopwords();
		process(inputPath, outputPath);
		
	}
	
	private static void process(File in, File out) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(in));
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		
		String line;
		while ((line = br.readLine()) != null) {
			bw.write(lemmasToString(tokenizer.getLemmas(line)));
		}
		br.close();
		bw.close();
	}
	
	private static String lemmasToString(List<String> list) {
		String result = "";
		for (String s : list) 
			result = result + " " + s;
		return result;
	}
	
	private static void setupStopwords() throws IOException {
		if (stopWords == null) {
			File path = new File(STOPWORDS_FILEPATH);
			List<String> lines = new ArrayList<>();
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line;
			
			while ((line = br.readLine()) != null)
				lines.add(line);
			
			stopWords = new HashSet<>(lines);
			br.close();
		}
		tokenizer = new Tokenizer(stopWords);
	}
}
