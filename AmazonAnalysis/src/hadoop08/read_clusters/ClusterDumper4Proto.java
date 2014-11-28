package hadoop08.read_clusters;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;

public class ClusterDumper4Proto {
	private static Logger log = LogManager.getLogger(ClusterDumper4Proto.class);

	public static void main(String[] args) {
		Path input = new Path(args[0]);

		long maxPointsPerCluster = Long.MAX_VALUE;

		readPoints(input, maxPointsPerCluster, new Configuration());
	}

	// adapted from org.apache.mahout.utils.clustering.ClusterDumper
	public static void readPoints(Path pointsPathDir, long maxPointsPerCluster, Configuration conf) {

		Set<Integer> clusters = new HashSet<>();

		SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable> dirIterable;
		dirIterable = new SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable>(
				pointsPathDir, PathType.LIST, PathFilters.logsCRCFilter(), conf);

		for (Pair<IntWritable, WeightedPropertyVectorWritable> record : dirIterable) {

			int clusterID = record.getFirst().get();
			clusters.add(clusterID);

			log.info("read in cluster: " + clusterID + ", total number of clusters so far: "
					+ clusters.size());
		}
	}
}
