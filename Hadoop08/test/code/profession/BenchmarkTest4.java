package code.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Scanner;


public class BenchmarkTest4 {
	
	public BufferedReader in; // Input file
	public BufferedWriter out; // Output 1, used for developing classifier
	public BufferedWriter out2; // Output 2, used for testing classifier (small subset of out1 with only names)
	public BufferedWriter out3; // Output 3, contains solution to out2 (includes professions of out2)
	
	
	public static void main(String[] args) throws IOException {
		setUp();
		testBenchmark();
		
	}
	public static void setUp() throws IOException {
		//Normally, the BufferedWriters would print to a file, not to console.
		BufferedReader in = new BufferedReader(new FileReader("real_profession.txt"));
		BufferedWriter out = new BufferedWriter(new FileWriter("profession_known.txt"));
		BufferedWriter out2 = new BufferedWriter(new FileWriter("profession_test.txt"));
		BufferedWriter out3 = new BufferedWriter(new FileWriter("profession_sol.txt"));
	
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
				Scanner console2 = new Scanner(new File("profession_known.txt"));
				Scanner console3 = new Scanner(new File("profession_test.txt"));
				Scanner console4 = new Scanner(new File("profession_sol.txt"));
				String test1 = "";
				String test2 = "";
				String test3 = "";
				String test4 = "";
				for (int i = 0; i < counter; i++) {
					test1 = console.nextLine();
					test2 = console2.nextLine();
					int hello = test1.compareTo(test2);
					System.out.print("The input and expected output should match: \r\n" + test1 + "\r\n" + test2 + "\r\n" + hello + "\r\n");
				}
				for (int i = 0; i < 2; i++) {
					test1 = console.nextLine();
					test3 = console3.nextLine();
					test4 = console4.nextLine();
					int hello = test1.compareTo(test4);
					boolean bye = test1.contains(test3);
					System.out.print("The input and expected output should match: \r\n" + test1 + "\r\n" + test4 + "\r\n" + hello + "\r\n");
					System.out.print("The input should contain the expected output: \r\n" + test1 + "\r\n" + test3 + "\r\n" + bye + "\r\n");
				}
				
	}
	

	public static void testBenchmark() throws IOException {
		BufferedReader in = new BufferedReader(new FileReader("test_profession.txt"));	 
		BufferedReader in2 = new BufferedReader(new FileReader("real_profession.txt")); 	 
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
		/**
		 * As shown in math.txt, the expected precision is 0.7142857142857143. 
		 */
	}
	

	public static String truncate(String line, int test) {
		String[] lineParts = line.split(" : ", 2);				//divides the line String into a String array of length 2.
		if (test == 0) {
			return lineParts[0];								//returns the String at index 0, which is the first half of truncation (the name).
		} else {
			return lineParts[1];
		}
	}
}
	