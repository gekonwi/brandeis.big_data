package code.benchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class Benchmark {

	public static void main (String[] args) throws IOException {
		// Call split() method if running for first time
		
		BufferedReader in = new BufferedReader(new FileReader("test_profession.txt"));	 //result of classification on prof_train.test.
		BufferedReader in2 = new BufferedReader(new FileReader("real_profession.txt"));  	 //solution to prof_train.test.
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
	
	public static void split() throws IOException { //method used to split profession_train into *train.known, *train.test, and *train.sol
		
		BufferedReader in = new BufferedReader(new FileReader("profession_train.txt"));
		BufferedWriter out = new BufferedWriter(new FileWriter("prof_train.known.txt"));
		BufferedWriter out2 = new BufferedWriter(new FileWriter("prof_train.test.txt"));
		BufferedWriter out3 = new BufferedWriter(new FileWriter("prof_train.sol.txt"));
		
		
		long counter = 0;
		
		String line = in.readLine();
		while (line != null) {								//loop as long as there is more input to be read
			if (counter < 600000) {							//first 600k reviews
				out.write(line + "\n");;  					//write output into prof_train.known.txt
				counter++;		
			}
			else {					
				out3.write(line + "\n");					//write solution into prof_train.sol.txt
				out2.write(truncate(line,0) + "\n");			//write output into prof_train.test.txt
			}
			line = in.readLine();							//move on to next line
		}
		
		//close the BufferedWriter and BufferedReader because we are done
				in.close();
				out.close();
				out2.close();
				out3.close();
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
