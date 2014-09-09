import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) throws IOException {
		/*
		 * Unfortunately my CS account was deactivated at the time I was doing
		 * the assignment. Thus I had to download the file and read it in
		 * locally.
		 */
		Path input = Paths.get("../../all_tail.txt");
		Path output = Paths.get("output.txt");

		transform(input, output);

		System.out.println("done");
	}

	private static void transform(Path input, Path output) throws IOException {
		InputStream is = null;
		Scanner sc = null;
		BufferedWriter bw = null;

		try {
			is = new FileInputStream(input.toFile());
			sc = new Scanner(is, "UTF-8");

			bw = new BufferedWriter(new FileWriter(output.toFile()));

			writeOutput(sc, bw);
		} finally {
			if (is != null)
				is.close();

			if (sc != null)
				sc.close();

			if (bw != null)
				bw.close();
		}
	}

	private static void writeOutput(Scanner sc, BufferedWriter bw)
			throws IOException {
		while (sc.hasNextLine()) {
			skipLines(sc, 6);
			String outputLine = getValue("review/score", sc.nextLine()) + "\t";

			skipLines(sc, 2);
			outputLine += getValue("review/text", sc.nextLine()) + "\n";

			skipLines(sc, 1);
			bw.write(outputLine);
		}

		bw.flush();
	}

	/**
	 * Skip the given number of lines in the given scanner.
	 * 
	 * @param sc
	 *            the Scanner
	 * @param numLines
	 *            the positive number of lines to be skipped
	 */
	private static void skipLines(Scanner sc, int numLines) {
		for (int i = 0; i < numLines; i++) {
			if (!sc.hasNextLine())
				break;

			sc.nextLine();
		}
	}

	/**
	 * The line is supposed to start with <code>lineHeading</code>, followed by
	 * a colon and a blank. The remaining part of the line is returned as the
	 * line value.
	 * 
	 * @param lineHeading
	 *            the prefix of the line (without colon and blank)
	 * @return suffix of the line, following <code>lineHeading + ": "</code>
	 */
	private static String getValue(String lineHeading, String line) {
		return line.substring(lineHeading.length() + 2);
	}
}
