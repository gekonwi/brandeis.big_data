package hadoop08.code.preprocess;

import static org.junit.Assert.*;

import java.io.*;
import java.util.Scanner;

import org.junit.*;

/**
 * 
 * @author Calvin Wang
 *
 */
public class PreProcessTest {
	
	public String getLemmaLines(int numLines, String filename) throws IOException {
		Tokenizer tokenizer = new Tokenizer(PreProcess.setupStopwords());
		Scanner scan = new Scanner(new File(
				"test_data/hadoop08/code/preprocess/" + filename));

		String result = "";
		String line = "";
		int count = 0;
		
		while (count <= numLines) {
			line = "";
			
			if (scan.hasNextLine()) 
				line = scan.nextLine();

			// append: lemmatized line
			result = result + 
			PreProcess.lemmasToString(tokenizer.getLemmas(line)).trim() + "\r\n";
			count++;
		}
		result = result.substring(0, result.lastIndexOf("\r\n"));
		scan.close();
		return result;
	}
	
	public String getLines(int numLines, Scanner s) {
		String line = "";
		int count = 0;
		while (count <= numLines && s.hasNextLine()) {
			line = line + s.nextLine().trim() + "\r\n";
			count++;
		}
		s.close();
		return line;
	}
	
	@Test
	public void TestOneLine() throws IOException {
		String line1 = getLemmaLines(1, "oneline_1.txt");
		String line1actual = getLines(1, new Scanner(new File(
				"test_data/hadoop08/code/preprocess/oneline_1_expect.txt")));
		
		String line2 = getLemmaLines(1, "oneline_2.txt");
		String line2actual = getLines(1, new Scanner(new File(
				"test_data/hadoop08/code/preprocess/oneline_2_expect.txt")));
		
		//System.out.println(line1);
		//System.out.println(line1actual);
		
		assertEquals(line1, line1actual);
		assertEquals(line2, line2actual);
	}
	
	@Test
	public void TestTwoLines() throws IOException {
		String line1and2 = getLemmaLines(2, "twoline_1and2.txt");
		String line1and2actual = getLines(2, new Scanner(new File(
				"test_data/hadoop08/code/preprocess/twoline_1and2_expect.txt")));

		//System.out.println(line1and2actual);
		
		assertEquals(line1and2, line1and2actual);
	}
	
}
