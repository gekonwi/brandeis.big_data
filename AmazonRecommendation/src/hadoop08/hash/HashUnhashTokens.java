package hadoop08.hash;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Hashes or unhashes the values of the first x tokens of a line in a
 * comma-separated csv file.
 * 
 * @author calvin
 * 
 */
public class HashUnhashTokens {
	public static void main(String[] args) throws IOException {
		// use arguments for filename input
		if (args.length != 5)
			throw new IllegalArgumentException(
					"need 5 parameters: inputPath, outputPath, [hash | unhash], HashMapPath, numTokens");
		
		if (!args[2].equals("hash") && !args[2].equals("unhash"))
			throw new IllegalArgumentException("invalid param: " + args[2]
					+ ", must be 'hash' or 'unhash'");

		int numTokens = Integer.parseInt(args[4]);
		
		if (args[2].equals("hash"))
			hash(new File(args[0]), new File(args[1]), args[2], new File(args[3]), numTokens);
		else
			unhash(new File(args[0]), new File(args[1]), args[2], new File(args[3]), numTokens);
	}

	private static void hash(File in, File out, String op, File hashMap, int numTokens) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(in));
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		BufferedWriter bhashw = new BufferedWriter(new FileWriter(hashMap));
		String line, builtLine, hashMapLine;

		while ((line = br.readLine()) != null) {

			String tokens[] = line.split(",");
			int translated[] = new int[numTokens];
			
			if (numTokens > tokens.length) {
				System.err.println("You can't hash more tokens then there are tokens in a line:\r\n" +
						"[" + numTokens + "] for [" + line);
				
				br.close();
				bw.close();
				bhashw.close();
				return;
			}

			builtLine = "";
			hashMapLine = "";
			for (int i = 0; i < tokens.length; i++){
				if (i < numTokens) {
					translated[i] = tokens[i].hashCode() + (Character.toString(tokens[i].charAt(0)).hashCode());
					
					builtLine += translated[i] + ",";
					hashMapLine += translated[i] + ":" + tokens[i] + "\r\n";
				} else {
					builtLine += tokens[i] + ",";
				}
				
			}
			
			builtLine = builtLine.substring(0, builtLine.lastIndexOf(","));
			
			bw.write(builtLine + "\r\n");
			bhashw.write(hashMapLine);

		}
		
		br.close();
		bw.close();
		bhashw.close();

	}
	

	private static void unhash(File in, File out, String op, File hashMap, int numTokens) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(in));
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		String line, builtLine, xlation;

		while ((line = br.readLine()) != null) {

			String tokens[] = line.split(",");
			
			if (numTokens > tokens.length) {
				System.err.println("You can't hash more tokens then there are tokens in a line:\r\n" +
						"[" + numTokens + "] for [" + line);
				
				br.close();
				bw.close();
				return;
			}
			builtLine = "";
			for (int i = 0; i < tokens.length; i++){
				
				if (i < numTokens) {

					xlation = findTranslation(tokens[i], hashMap);
					
					if (xlation == null)
						System.err.println("Translation for " + tokens[i] + " not found.");
					else
						builtLine += xlation + ",";
					
				} else {
					builtLine += tokens[i] + ",";
				}
			}
			
			builtLine = builtLine.substring(0, builtLine.lastIndexOf(","));
			
			bw.write(builtLine + "\r\n");

		}
		
		br.close();
		bw.close();

	}
	
	private static String findTranslation(String token, File hashMap) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(hashMap));
		String line, value;
		
		System.out.println("reading file " + hashMap.getName());
		
		while ((line = br.readLine()) != null) {
			
			String[] keyval = line.split(":");
			
			System.out.println("finding translation for " + keyval[0]);
			
			if (keyval[0].equals(token)) {
				value = keyval[1];
				System.out.println("for token " + token + " found key " + keyval[0] + " returning value " + value);
				br.close();
				return value;
			}
			
		}
		br.close();
		return null;
	}
}
