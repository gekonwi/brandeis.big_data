package hadoop08.read_clusters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class BlankReviewCounter {
	private static Logger log = LogManager.getLogger(BlankReviewCounter.class);

	/**
	 * Find out if exactly 109 reviews contain stopwords -- and thus, 109
	 * reviews have 0 words post-lemmatization.
	 * 
	 * @author Calvin
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		File input = new File(args[0]);

		findBlanks(input);
	}

	/**
	 * Find lines where there contains only a number and no proceeding words.
	 * @param input
	 *            A path to a file in the form:
	 *            	<line number> <space separated lemmatized words>
	 * @throws IOException 
	 */
	private static void findBlanks(File input) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(input));
		String line;
		long count = 0;
		
		while ((line = br.readLine()) != null) {
			String[] parts = line.split("\t");
			
			if (parts.length == 1) {
				log.info("Empty line #" + ++count + " found:\r\n\t[" + line + "]");
			}
		}
		log.info("Total empty lines: " + count);
		br.close();
		
	}
}
