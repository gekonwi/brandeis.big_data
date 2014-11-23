package hadoop08;

import hadoop08.read_clusters.ClusterDumper;

import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Main {

	private static Logger log = LogManager.getLogger(Main.class);

	public static void main(String[] args) throws IOException {
		String command = args[0];
		String[] remainingArgs = Arrays.copyOfRange(args, 1, args.length);

		switch (command) {
		case "clusterdump":
			log.info("running " + ClusterDumper.class.getName() + ".main(...)");
			ClusterDumper.main(remainingArgs);
		default:
			log.info("don't know what to do with command '" + command + "'");
		}
	}

}
