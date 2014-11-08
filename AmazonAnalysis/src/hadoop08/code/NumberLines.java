import java.io.*;

public class NumberLines {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("preprocess-pa3out/part-r-00000"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("preprocess-pa3out/numberedLines"));

		String line;
		long lineNum = 0;
		System.out.println("Starting...");
		while ((line = br.readLine()) != null) {
			if (lineNum%1000 == 0)
				System.out.println(lineNum);
			bw.write(lineNum + "\t" + line + "\r\n");
			lineNum++;
		}

		br.close();
		bw.close();
	}
}