package hadoop08.SetClusterDumpQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class SetClusterDumpQueue {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// we can't specify the queue name otherwise
		conf.set("mapreduce.job.queuename", "hadoop08");

		ClusterDumper cd = new ClusterDumper();
		cd.setConf(conf);

		cd.run(args);
	}
}