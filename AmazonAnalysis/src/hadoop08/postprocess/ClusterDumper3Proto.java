package hadoop08.postprocess;

import java.util.List;
import java.util.Map;

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ClusterDumper3Proto {
	private static Logger log = LogManager.getLogger(ClusterDumper3Proto.class);

	public static void main(String[] args) {
		Path input = new Path(args[0]);
		int printFirstN = Integer.parseInt(args[1]);

		readPoints(input, printFirstN, new Configuration());
	}

	// adapted from org.apache.mahout.utils.clustering.ClusterDumper
	public static Map<Integer, List<WeightedPropertyVectorWritable>> readPoints(Path pointsPathDir,
			long maxPointsPerCluster, Configuration conf) {

		Map<Integer, List<WeightedPropertyVectorWritable>> result = Maps.newTreeMap();

		SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable> dirIterable;
		dirIterable = new SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable>(
				pointsPathDir, PathType.LIST, PathFilters.logsCRCFilter(), conf);

		for (Pair<IntWritable, WeightedPropertyVectorWritable> record : dirIterable) {

			int clusterID = record.getFirst().get();
			List<WeightedPropertyVectorWritable> pointList = result.get(clusterID);

			if (pointList == null) {
				pointList = Lists.newArrayList();
				result.put(clusterID, pointList);
			}

			if (pointList.size() < maxPointsPerCluster) {
				pointList.add(record.getSecond());
			}

			log.info("read in cluster: " + clusterID + ", points: " + pointList);
		}
		return result;
	}
}
