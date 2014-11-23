package hadoop08.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

import com.jcraft.jsch.*;

/**
 * Class to parse through all.txt and extract relevant fields of each review
 * (productId, userId, score).
 * 
 * @author Calvin Wang
 * 
 */
public class RecommendPreProcess {
	public static void main(String[] args) throws FileNotFoundException, IOException,
			JSchException, SftpException {
		if (args.length != 1)
			throw new IllegalArgumentException("need 1 and only 1 parameter: outputPath");

		/*
		 * Inspiration for the following JSch code:
		 * http://stackoverflow.com/a/2690861
		 */

		JSch jsch = new JSch();

		Session session = jsch.getSession("hadoop08", "129.64.2.200", 22);
		session.setConfig("StrictHostKeyChecking", "no");
		session.setPassword("olj0O}an6taR");

		session.connect();

		Channel channel = session.openChannel("sftp");
		channel.connect();

		ChannelSftp sftpChannel = (ChannelSftp) channel;

		InputStream in = sftpChannel.get("/home/o/class/cs129a/assignment4/all.txt");
		File outputPath = new File(args[0]);

		// let's process the streamed-in file
		process(in, outputPath);

		sftpChannel.exit();
		session.disconnect();
	}

	/**
	 * Place extracted relevant fields (productId, userId, score) on one line
	 * like so: "productId	userId score"
	 * 
	 * @param in
	 *            InputStream file
	 * @param out
	 *            a new file to be created with output
	 * @throws IOException
	 */
	private static void process(InputStream in, File out) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		String line, tag;
		String builtLine = "";
		Scanner scan;
		long count = 0;

		while ((line = br.readLine()) != null) {
			scan = new Scanner(line);

			// for progress checks during execution
			if (++count % 1000000 == 0)
				System.out.println(count + " reviews scanned");

			// is a line and not a review-separating linebreak
			if (scan.hasNext()) {
				tag = scan.next();

				if (tag.equals("product/productId:") || tag.equals("review/userId:")
						|| tag.equals("review/score:")) {

					String[] lineParts = line.split(" ");

					// append the data to builtLine
					if (tag.equals("product/productId:"))
						builtLine += lineParts[1] + "\t";
					if (tag.equals("review/userId:"))
						builtLine += lineParts[1] + " ";
					if (tag.equals("review/score:"))
						builtLine += lineParts[1];

				}
				// means we've reached end of a review block; add an extra space
				if (tag.equals("\r\n")) {
					builtLine += "\r\n";
				}
			} else {
				// is end, write
				bw.write(builtLine + "\r\n");
				builtLine = "";
				// progress check
				if (count % 100000 == 0)
					System.out.println(count + " reviews condensed");
			}
			scan.close();
		}
		br.close();
		bw.close();
	}
}
