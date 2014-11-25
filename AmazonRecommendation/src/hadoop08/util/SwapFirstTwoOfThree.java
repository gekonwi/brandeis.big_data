package hadoop08.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class SwapFirstTwoOfThree {
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			throw new IllegalArgumentException("Need: input, output");
		}

		BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(args[1])));
		String line;
		long count = 0;
		
		while ((line = br.readLine()) != null) {
			String builtLine = "";
			String[] tokens = line.split(",");
			
			builtLine = tokens[1] + "," + tokens[0] + "," + tokens[2] + "\r\n";
			bw.write(builtLine);
			
			if (++count%1000000 == 0)
				System.out.println("Swapped in " + count + " lines so far");
		}		
		
		br.close();
		bw.close();
	}
}
