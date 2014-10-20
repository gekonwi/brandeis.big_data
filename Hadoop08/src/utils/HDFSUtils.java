package utils;

import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

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
	 * @param config
	 * @return a list with one entry for each line in the file
	 * @throws IOException
	 */
	public static List<String> readLines(Path filePath, Configuration config) throws IOException {
		List<String> result = new ArrayList<>();

		BufferedReader br = getFileReader(filePath, config);

		String line;
		while ((line = br.readLine()) != null) {
			result.add(line);
		}

		br.close();

		return result;
	}

	public static BufferedReader getFileReader(Path filePath, Configuration config)
			throws IOException {
		FileSystem fs = filePath.getFileSystem(config);
		return new BufferedReader(new InputStreamReader(fs.open(filePath)));
	}

	public static String addCacheFile(Job job, String path) throws URISyntaxException {
		String workingDir = "/user/hadoop08/";

		if (!path.startsWith("/"))
			path = workingDir + path;

		job.addCacheFile(new URI(path));

		return workingDir;
	}
}
