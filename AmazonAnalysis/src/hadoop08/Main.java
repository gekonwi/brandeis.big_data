package hadoop08;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

	private static Logger log = LogManager.getLogger(Main.class);

	public static void main(String[] args) throws IOException {
		org.apache.log4j.BasicConfigurator.configure();
		log.info("no default behavior implemented yet. You could run a specific class, e.g. "
				+ "hadoop08.read_clusters.ClusterDumper clusterDir outputDir");
	}

}
