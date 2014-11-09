package hadoop08;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import number_lines.copy.NumberLines;

public class Main {

	public static void main(String[] args) throws IOException {
		Path input = Paths.get("test_data", "text_files", "sample_text");
		Path numberedInput = Paths.get("output", "001", "numbered_input");
		NumberLines.run(input, numberedInput);
	}

}
