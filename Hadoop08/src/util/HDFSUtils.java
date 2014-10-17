package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
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
	 *            the full (absolute or relative) path to the file to be read
	 * @return a HashMap<String, Integer> where the key is a profession and the
	 *         value is the frequency of the profession from the input file
	 * @throws IOException
	 */
	public static HashMap<String, Integer> readProfessions(String filePath) throws IOException {
		HashMap<String, Integer> result = new HashMap<String, Integer>();

		Path professionsPath = new Path(filePath);
		FileSystem fs = professionsPath.getFileSystem(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(professionsPath)));
		// below is for testing purposes
		// BufferedReader br = new BufferedReader(new FileReader(filePath));

		String line;
		while ((line = br.readLine()) != null) {
			String professions = line.substring(line.indexOf(": ") + 2, line.length());
			String[] professionsArr = professions.split(", ");
			for (String s : professionsArr) {
				if (result.containsKey(s))
					result.put(s, result.get(s) + 1);
				else
					result.put(s, 1);
			}
		}

		br.close();

		return result;
	}

	// below is for testing purposes
//	public static void main(String[] args) throws IOException {
//		HashMap<String, Integer> temp = readProfessions("Profession_test.txt");
//		for (String s : temp.keySet()) {
//			System.out.println(s + " " + temp.get(s));
//		}
//	}

	// don't know if we need really, but here is a method for counting the lines of an input file
	// http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	public static int countLines(String filePath) throws IOException {
		Path professionsPath = new Path(filePath);
		FileSystem fs = professionsPath.getFileSystem(new Configuration());
		InputStream is = fs.open(professionsPath);
		try {
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
	}
	
	/**
	 * Write professions count list out to file, professions_count.txt
	 * @param filePath
	 * @throws IOException
	 */
	public static void writeProfessions(String filePath) throws IOException{
		File file = new File("professions_count.txt");
		if(!file.exists())
			file.createNewFile();
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		
		HashMap<String, Integer> professions = readProfessions(filePath);
		
		for(String s:professions.keySet())
			bw.write(s + " " + professions.get(s));
		
		bw.close();
	}
}
