package hadoop08.hash;

import java.io.*;

public class RemoveNoise {
	public static void main(String[] args) throws IOException {
		
		if (args.length != 2)
			System.err.println("Need 2 parameters: input file to denoise, and output file");
		
		File in = new File(args[0]);
		File out = new File(args[1]);
		
		denoise(in, out);
	}
	
	public static void denoise(File in, File out) throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(in));
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		String line;
		long count = 0;
		
		System.out.println("Removing noise...");
		
		while ((line = br.readLine()) != null) {
			
			String newLine = line.replaceAll("/\\t|\\[|\\:\\d\\.\\d|\\]/g","");
			newLine = newLine.replaceAll("]","");
			newLine = newLine.replaceAll("\t",",");
			
			bw.write(newLine + "\n");
			if (++count%100_000 == 0) 
				System.out.println(count+" lines so far...");
		}
		
		br.close();
		bw.close();
	}
}
