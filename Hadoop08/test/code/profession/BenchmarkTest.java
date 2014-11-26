package code.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.Scanner;

import org.junit.Before;
import org.junit.Test;
public class BenchmarkTest {
	
	@Test 
	public void testSplit() {
		//Tests to see if file splits correctly
		BufferedReader in = new BufferedReader(new FileReader("real_profession"));		//original file
		BufferedWriter outKnown = new BufferedWriter(new FileWriter("known_profession"));	//part of original file for classifier
		BufferedWriter outTest = new BufferedWriter(new FileWriter("test_profession.txt"));	//part of original without professions
		BufferedWriter outSol = new BufferedWriter(new FileWriter("sol_profession.txt"));	//part of original with professions
		long counter = 0;
		
		String line = in.readLine();
		while (line != null) {							//loop as long as there is more input to be read
			if (counter < 5) {						//first 5 (out of 7) profession index.
				outKnown.write(line + "\n");				//This is the rough part. The test file is 7 lines
				counter++;						//long, so I just print manually to count.
			}
			else {	
				outSol.write(line + "\n");
				outTest.write(truncate(line,0) + "\n");
			}
			line = in.readLine();						//move on to next line
		}
											//close the BufferedWriter and BufferedReader 
		in.close();
		outKnown.close();
		outTest.close();
		outSol.close();

		Scanner console = new Scanner(new File("real_profession.txt"));		//scans original file (solution)
		Scanner console2 = new Scanner(new File("known_profession.txt"));	//scans known profession file (used for classifier)
		Scanner console3 = new Scanner(new File("test_profession.txt"));	//scans test file (doesn't contain professions)
		Scanner console4 = new Scanner(new File("sol_profession.txt"));		//scans solution file (contains professions)
		
		String lineReal = "";							//line by line comparison of real, known, test, sol
		String lineKnown = "";
		String lineTest = "";
		String lineSol = "";
		
		for (int i = 0; i < counter; i++) {					//same here, manually split which lines are being 
			lineReal = console.nextLine();					//compared with a for loop. 
			lineKnown = console2.nextLine();				
			int equals = testReal.compareTo(Known);
			assertEquals("The input and expected output should match: \r\n" + lineReal + "\r\n" + lineKnown + "\r\n",0,equals);
		}
		
		for (int i = 0; i < 2; i++) {
			lineReal = console.nextLine();					
			lineTest = console3.nextLine();
			lineSol = console4.nextLine();
			int equals = testReal.compareTo(lineSol);			//lines from solution file should match original
			boolean contains = lineReal.contains(lineTest);			//lines from test should be a substring of original
			assertEquals("The input and expected output should match: \r\n" + lineReal + "\r\n" + lineSol + "\r\n",0,equals);
			assertTrue("The input should contain the expected output: \r\n" + lineReal + "\r\n" + lineTest + "\r\n",contains);
		}
	}
	
	@Test
	public void testBenchmark() {
		//tests to see if precision is correct.
		BufferedReader in = new BufferedReader(new FileReader("test_profession.txt"));	 
		BufferedReader in2 = new BufferedReader(new FileReader("sol_profession.txt")); 	 
		String line = in.readLine(); 						//line of result.
		String lineSol = in2.readLine(); 					//line of solution key.
		double correct = 0; 
		double counter = 0;
		
		while (line !=null) {
			String profList = truncate(line, 1); 				//string of professions in result line.
			for (String guess : profList.split(", "))
				   if(lineSol.contains(guess)) {
				      correct++;
				      break;
				   }
			counter++;
			line = in.readLine();
			lineSol = in2.readLine();
		}
		double precision = correct/counter;
		double check = 5.0/7.0;							//manually calculated correct = 5, total = 7
		assertEquals("The expected output should match the actual: \r\n", precision, check);
		
	}
	
	@Test
	public static String truncate(String line, int test) {
		String[] lineParts = line.split(" : ", 2);				//divides the line into a String array of length 2.
		if (test == 0) {
			return lineParts[0];						//string at index 0 is the first half (the name)
		} else {								
			return lineParts[1];						//string at index 1 is the second half (professions)
		}
	}
}
	
