package hadoop08.fuzzy;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;

public class FuzzyCMeans {
	public static void main(String[] args) throws Exception {
		/**
		 * a file path string to a directory containing the input data set a
		 * SequenceFile(WritableComparable, VectorWritable). The sequence file
		 * key is not used.
		 */
		Path input = Paths.get("output/seqdirectory/003");

		/**
		 * a file path string to a directory containing the initial clusters, a
		 * SequenceFile(key, SoftCluster | Cluster | Canopy). Fuzzy k-Means
		 * SoftClusters, k-Means Clusters and Canopy Canopies may be used for
		 * the initial clusters.
		 */
		Path initialClusters = Paths.get("input/fuzzy/initial_clusters");

		/**
		 * a file path string to an empty directory which is used for all output
		 * from the algorithm.
		 */
		Path output = Paths.get("output/fuzzy/001");

		/**
		 * a double value used to determine if the algorithm has converged
		 * (clusters have not moved more than the value in the last iteration).
		 * Default is 0.5.
		 */
		double convergenceDelta = 0.5;

		/**
		 * the maximum number of iterations to run, independent of the
		 * convergence specified
		 */
		int maxIterations = 100; // TODO no clue if this value makes sense

		/**
		 * the "fuzzyness" argument, a double > 1. For m equal to 2, this is
		 * equivalent to normalising the coefficient linearly to make their sum
		 * 1. When m is close to 1, then the cluster center closest to the point
		 * is given much more weight than the others, and the algorithm is
		 * similar to k-means.
		 */
		double m = 3.0;

		// FuzzyKMeansDriver.run(input, initialClusters, output,
		// convergenceDelta, maxIterations, m,
		// runClustering, emitMostLikely, threshold, runSequential);

		Configuration conf = new Configuration();

		// we can't specify the queue name otherwise
		conf.set("mapreduce.job.queuename", "hadoop08");

		// ClusterDumper cd = new ClusterDumper();
		// cd.setConf(conf);
		//
		// ArrayList<String> cdArgs = new ArrayList<>(15);
		// cdArgs.add("--input");
		// cdArgs.add("output/kmeans/002/clusters-2-final/part-r-00000");
		// cdArgs.add("--output");
		// cdArgs.add("output/decoded/kmeans/001");
		// cdArgs.add("--dictionary");
		// cdArgs.add("output/seq2sparse/002/dictionary.file-0");
		// cdArgs.add("--dictionaryType");
		// cdArgs.add("sequencefile");
		// cdArgs.add("--numWords");
		// cdArgs.add("20");
		// cdArgs.add("--substring");
		// cdArgs.add("100");
		// cdArgs.add("--pointsDir");
		// cdArgs.add("output/kmeans/002/clusteredPoints");
		// cdArgs.add("--evaluate");
		//
		// cd.run(cdArgs.toArray(new String[cdArgs.size()]));
	}
}
