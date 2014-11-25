package hadoop08.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class KMeansRandomOutput {

	private static BufferedWriter bw;
	private final static Random rand = new Random();
	
	public static void main(String[] args) throws IOException {
		generateKMeansOutput();
		generateFKMeansOutput();
	}

	public static void generateKMeansOutput() throws IOException{
		bw = new BufferedWriter(new FileWriter("k_means_output.txt"));
		
		for (int i = 0; i < 2611908; i++) {
			bw.write(Integer.toString(rand.nextInt(10)+1));
			bw.newLine();
		}
		
		bw.close();
	}
	
	public static void generateFKMeansOutput() throws IOException{
		bw = new BufferedWriter(new FileWriter("fk_means_output.txt"));
		
		for (int i = 0; i < 2611908; i++) {
			int[] probs = new int[10];
			int sum = 0;
			for (int j = 0; j < 10; j++){
				probs[j] = rand.nextInt(1000000) + 1;
				sum += probs[j];
			}
			
			bw.write(probsToString(probs, sum));
			bw.newLine();
		}
		
		bw.close();
	}
	
	public static String probsToString(int[] probs, int sum){
		String result = "";
		
		for (int i = 0; i < 10; i++){
			if (i == 9)
				result += (i+1) + ":" + ((double) probs[i]/sum);
			else
				result += (i+1) + ":" + ((double) probs[i]/sum) + ", ";
		}
		
		return result;
	}
}
