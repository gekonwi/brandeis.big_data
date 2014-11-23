package hadoop08.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class RemoveUniques {
	public static void main(String[] args) throws IOException {
		if (args.length != 2)
			throw new IllegalArgumentException("need 2 parameters: inputPath outputPath");

		seekAndOmit(new File(args[0]), new File(args[1]));
	}

	public static void seekAndOmit(File in, File out) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(in));
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));

		Scanner prevScan, currScan;
		String previous, current;
		String prev, curr;
		long count = 0;
		long skipped = 0;
		boolean diff = true;

		// reads into mem first 2 lines so 'previous' and 'current' work
		if ((previous = br.readLine()) != null) {
		}

		while ((current = br.readLine()) != null) {
			prevScan = new Scanner(previous);
			currScan = new Scanner(current);

			prev = prevScan.next();
			curr = currScan.next();
			
			// previous line is different than current line, and previous line
			// was also different from its previous line
			if (!prev.equals(curr)) {
				if (diff) {
					// previous line must be unique, so we don't write it out
					System.out.println("skipped " + ++skipped);
				} else {
					// previous line was the same as its previous, but just diff from current
					diff = true;
					// this means previous was the last of its type
					bw.write(previous + "\r\n");
					count++;
				}
			} else {
				bw.write(previous + "\r\n");
				// previous line is same as current line
				if (diff) {
					// means previous is first of its type
					diff = false;
					count++;
				}

				if (count % 1000000 == 0)
					System.out.println("written " + count);
			}
			previous = current;

			prevScan.close();
			currScan.close();
		}
		
		// at the end, we have the last line not accounted for (since we print previouses)
		if (!diff)
			bw.write(previous + "\r\n");
		else 
			System.out.println("skipped " + ++skipped);

		br.close();
		bw.close();
	}
}
