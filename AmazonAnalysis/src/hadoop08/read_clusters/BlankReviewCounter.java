package hadoop08.read_clusters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
		File logOutput = new File(args[1]);

		findBlanks(input, logOutput);
	}

	/**
	 * Find lines where there contains only a number and no proceeding words.
	 * @param input
	 *            A path to a file in the form:
	 *            	<line number> <space separated lemmatized words>
	 * @throws IOException 
	 */
	private static void findBlanks(File input, File logOutput) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(input));
		FileWriter fw = new FileWriter(logOutput);
		String line;
		String lineNumbers = "";
		long count = 0;
		
		while ((line = br.readLine()) != null) {
			String[] parts = line.split("\t");
			
			if (parts.length == 1) {
				log.info("Empty line #" + ++count + " found:\r\n\t[" + line + "]");
				lineNumbers += parts[0] + ",";
			}
		}
		log.info("Total empty lines: " + count);
		fw.write(lineNumbers);
		fw.close();
		br.close();
	}
}
