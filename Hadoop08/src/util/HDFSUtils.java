package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
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
}
