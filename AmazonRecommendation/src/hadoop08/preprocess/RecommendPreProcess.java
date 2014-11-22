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

public class RecommendPreProcess {
	public static void main(String[] args) throws FileNotFoundException, IOException, JSchException, SftpException {
		if (args.length != 1)
			throw new IllegalArgumentException(
					"need parameter: outputPath");
		
		JSch jsch = new JSch();
		
		Session session = jsch.getSession("hadoop08", "129.64.2.200", 22);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setPassword("olj0O}an6taR");

		session.connect();

		Channel channel = session.openChannel( "sftp" );
		channel.connect();

		ChannelSftp sftpChannel = (ChannelSftp) channel;

		InputStream in = sftpChannel.get( "/home/o/class/cs129a/assignment4/all.txt" );
		File outputPath = new File(args[0]);
		
		process(in, outputPath);
		
		sftpChannel.exit();
		session.disconnect();
	}
	
	public static void process(InputStream in, File out) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		BufferedWriter bw = new BufferedWriter(new FileWriter(out));
		String line, tag;
		String builtLine = "";
		Scanner scan;
		long count = 0;

		while ((line = br.readLine()) != null) {
			scan = new Scanner(line);
			
			if (scan.hasNext()) {
				//is a line
				tag = scan.next();
				
				if (tag.equals("product/productId:") || tag.equals("review/userId:") || tag.equals("review/score:")) {
					
					String[] lineParts = line.split(" ");
					
					if (tag.equals("product/productId:"))
						builtLine += lineParts[1] + "\t";
					if (tag.equals("review/userId:"))
						builtLine += lineParts[1] + " ";
					if (tag.equals("review/score:"))
						builtLine += lineParts[1];
					
				}
				if (tag.equals("\r\n")) {
					builtLine += "\r\n";
				}
			} else {
				//is end, write
				bw.write(builtLine + "\r\n");
				builtLine = "";
			}
			
			if (++count%1000000 == 0)
				System.out.println(count);
		}
		br.close();
		bw.close();
	}
}
