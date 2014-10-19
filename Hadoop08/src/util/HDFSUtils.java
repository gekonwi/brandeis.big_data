package util;

import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 * @author Steven Hu, stevenhh@brandeis.edu
 * 
 */
public class HDFSUtils {

	/**
	 * Read all lines of the given file from HDFS.
	 * 
	 * @param filePath
	 *            the path to the file to be read
	 * @return a list with one entry for each line in the file
	 * @throws IOException
	 */
	public static List<String> readLines(Path filePath) throws IOException {
		List<String> result = new ArrayList<>();

		FileSystem fs = filePath.getFileSystem(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));

		String line;
		while ((line = br.readLine()) != null) {
			result.add(line);
		}

		br.close();

		return result;
	}

	/**
	 * Read all lines from a given professions training file in HDFS
	 * 
	 * @param fileLines
	 *            each entry has to have the format:
	 *            <p>
	 *            article name : profession1, profession2, ... <br>
	 *            <p>
	 * @return a HashMap<String, Integer> where the key is a profession and the
	 *         value is the frequency of the profession from the input file
	 */
	public static HashMap<String, Integer> getProfessionCounts(List<String> fileLines) {
		HashMap<String, Integer> result = new HashMap<String, Integer>();

		for (String line : fileLines) {
			String[] professions = line.split(" : ")[1].split(", ");
			for (String p : professions)
				if (result.containsKey(p))
					result.put(p, result.get(p) + 1);
				else
					result.put(p, 1);
		}

		return result;
	}
}
