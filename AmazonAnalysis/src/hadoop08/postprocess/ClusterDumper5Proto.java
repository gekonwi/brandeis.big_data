package hadoop08.postprocess;

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
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

public class ClusterDumper5Proto {
	private static Logger log = LogManager.getLogger(ClusterDumper5Proto.class);

	public static void main(String[] args) {
		Path input = new Path(args[0]);

		readPoints(input, new Configuration());
	}

	// adapted from org.apache.mahout.utils.clustering.ClusterDumper
	public static void readPoints(Path pointsPathDir, Configuration conf) {

		SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable> dirIterable;
		dirIterable = new SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable>(
				pointsPathDir, PathType.LIST, PathFilters.logsCRCFilter(), conf);

		Set<String> vectorIDs = new HashSet<>();
		long count = 0;

		for (Pair<IntWritable, WeightedPropertyVectorWritable> record : dirIterable) {
			count++;

			Vector vector = record.getSecond().getVector();
			String vectorID = ((NamedVector) vector).getName();
			vectorIDs.add(vectorID);

			log.info(count + ": read in vector: " + vectorID + ", total number of vectors so far: "
					+ vectorIDs.size());
		}
	}
}
