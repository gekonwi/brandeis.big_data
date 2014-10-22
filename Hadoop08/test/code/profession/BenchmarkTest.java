package code.benchmark;

import static org.junit.Assert.assertEquals;

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
	
	BufferedReader in; // Input file
	BufferedWriter out; // Output 1, used for developing classifier
	BufferedWriter out2; // Output 2, used for testing classifier (small subset of out1 with only names)
	BufferedWriter out3; // Output 3, contains solution to out2 (includes professions of out2)
	
	
	@Before
	public void setUp() {
		//Normally, the BufferedWriters would print to a file, not to console.
		BufferedReader in = new BufferedReader(new FileReader("real_profession"));
		BufferedWriter out = new BufferedWriter(new FileWriter("known_profession"));
		BufferedWriter out2 = new BufferedWriter(new FileWriter("test_profession.txt"));
		BufferedWriter out3 = new BufferedWriter(new FileWriter("sol_profession.txt"));
	}
	
	@Test 
	public void testSplit() {
		//Tests to see if file splits correctly
		long counter = 0;
		
		String line = in.readLine();
		while (line != null) {								//loop as long as there is more input to be read
			if (counter < 5) {								//first 5 profession index.
				out.write(line + "\n");
				counter++;		
			}
			else {	
				out3.write(line + "\n");
				out2.write(truncate(line,0) + "\n");
			}
			line = in.readLine();							//move on to next line
		}
		
		//close the BufferedWriter and BufferedReader because we are done
		in.close();
		out.close();
		out2.close();
		out3.close();
		Scanner console = new Scanner(new File("real_profession.txt"));
		Scanner console2 = new Scanner(new File("known_profession.txt"));
		Scanner console3 = new Scanner(new File("test_profession.txt"));
		Scanner console4 = new Scanner(new File("sol_profession.txt"));
		String test1 = "";
		String test2 = "";
		String test3 = "";
		String test4 = "";
		for (int i = 0; i < counter; i++) {
			test1 = console.nextLine();
			test2 = console2.nextLine();
			int hello = test1.compareTo(test2);
			assertEquals("The input and expected output should match: \r\n" + test1 + "\r\n" + test2 + "\r\n",0,hello);
		}
		for (int i = 0; i < 2; i++) {
			test1 = console.nextLine();
			test3 = console3.nextLine();
			test4 = console4.nextLine();
			int hello = test1.compareTo(test4);
			boolean bye = test1.contains(test3);
			assertEquals("The input and expected output should match: \r\n" + test1 + "\r\n" + test4 + "\r\n",0,hello);
			assertTrue("The input should contain the expected output: \r\n" + test1 + "\r\n" + test3 + "\r\n",bye);
		}
	}
	
	@Test
	public void testBenchmark() {
		BufferedReader in = new BufferedReader(new FileReader("test_profession.txt"));	 
		BufferedReader in2 = new BufferedReader(new FileReader("sol_profession.txt")); 	 
		String line = in.readLine(); //line of result.
		String line2 = in2.readLine(); //line of solution key.
		double correct = 0; 
		double counter = 0;
		while (line !=null) {
			String profList = truncate(line, 1); //string of professions in result line.
			for (String guess : profList.split(", "))
				   if(line2.contains(guess)) {
				      correct++;
				      break;
				   }
			counter++;
			line = in.readLine();
			line2 = in2.readLine();
		}
		double precision = correct/counter;
		System.out.println("Your precision is " + precision);
		
	}
	
	@Test
	public static String truncate(String line, int test) {
		String[] lineParts = line.split(" : ", 2);				//divides the line String into a String array of length 2.
		if (test == 0) {
			return lineParts[0];								//returns the String at index 0, which is the first half of truncation (the name).
		} else {
			return lineParts[1];
		}
	}
}
	