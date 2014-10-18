package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

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
	 *            the full (absolute or relative) path to the file to be read
	 * @return a set with one entry for each (unique) line
	 * @throws IOException
	 */
	public static HashSet<String> readLines(String filePath) throws IOException {
		HashSet<String> result = new HashSet<>();

		Path peoplePath = new Path(filePath);
		FileSystem fs = peoplePath.getFileSystem(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(peoplePath)));

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
	 * @param filePath
	 *            the full (absolute or relative) path to the file to be read.
	 *            It has to have the format:
	 *            <p>
	 *            article1 name : profession1, profession2, ... <br>
	 *            article2 name : profession3 ... ...
	 *            <p>
	 * @return a HashMap<String, Integer> where the key is a profession and the
	 *         value is the frequency of the profession from the input file
	 * @throws IOException
	 */
	public static HashMap<String, Integer> getProfessionCounts(String filePath) throws IOException {
		HashMap<String, Integer> result = new HashMap<String, Integer>();

		for (String line : readLines(filePath)) {
			String[] professions = line.split(" : ")[1].split(", ");
			for (String p : professions)
				if (result.containsKey(p))
					result.put(p, result.get(p) + 1);
				else
					result.put(p, 1);
		}

		return result;
	}

	/**
	 * Write professions count list out to file, professions_count.txt
	 * 
	 * @param filePath
	 * @throws IOException
	 */
	public static void writeProfessionCounts(String filePath) throws IOException {
		Path outputFile = new Path("professions_count.txt");
		
		FileSystem fs = FileSystem.get(new Configuration());
		
		if (!fs.exists(outputFile)) {
			fs.create(outputFile);
		} else {
			System.err.println("Output file '" + outputFile.toString() + "' already exists.");
			System.exit(1);
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile.getName()));
	
		HashMap<String, Integer> professions = getProfessionCounts(filePath);
	
		for (String s : professions.keySet())
			bw.write(s + "\t" + professions.get(s));
	
		bw.close();
	}
}
