package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This class just prefixes each line in the given input with the line number
 * and a <code>\t</code> character.
 * 
 * @author Shlomo Konwisser, gekonwi@gmail.com
 * 
 */
public class NumberLines {
	public static void main(String[] args) throws IOException {
		run(Paths.get(args[0]), Paths.get(args[1]));
	}

	public static void run(Path input, Path output) throws IOException {
		Charset utf8 = Charset.forName("UTF-8");
		BufferedReader br = Files.newBufferedReader(input, utf8);

		Files.createDirectories(output.getParent());
		BufferedWriter bw = Files.newBufferedWriter(output, utf8);

		long lineNum = 0;

		String line;
		while ((line = br.readLine()) != null) {
			lineNum++;
			bw.write(lineNum + "\t" + line + "\n");
		}

		bw.flush();
		bw.close();

		br.close();
	}
}
