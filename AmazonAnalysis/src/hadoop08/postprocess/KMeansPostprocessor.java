package hadoop08.postprocess;

import hadoop08.preprocess.ToSequenceFileMR;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

/**
 * 
 * @author Shlomo Georg Konwisser, gekonwi@brandeis.edu
 * 
 */
public class KMeansPostprocessor {

	private static Logger log = LogManager.getLogger(KMeansPostprocessor.class);

	/**
	 * 
	 * @param args
	 *            <code>kMeansOutputDir</code>: containing the
	 *            <code>clusteredPoints</code> directory <br>
	 *            <code>postProcessOutput</code>: target of this post-processing<br>
	 *            <code>maxReviewID</code>: total number of original reviews
	 * @throws IOException
	 * 
	 */
	public static void main(String[] args) throws IOException {
		Path kMeansOutputDir = new Path(args[0]);
		Path postProcessOutput = new Path(args[1]);
		long maxReviewID = Long.parseLong(args[2]);

		Map<Long, Integer> reviewCategoryMap = readClustering(kMeansOutputDir);

		Map<Integer, Integer> clusterScalingMap = getClusterScaling(reviewCategoryMap);

		scaleClusters(reviewCategoryMap, clusterScalingMap);

		addMissingReviews(reviewCategoryMap, clusterScalingMap.size(), maxReviewID);

		writeSorted(reviewCategoryMap, postProcessOutput);
	}

	/**
	 * Adds missing review-> cluster assignments. Each added cluster is a random
	 * number between 1 and <code>maxClusterID</code>. In the end the provided
	 * <code>reviewCategoryMap</code> will contain <code>maxClusterID</code>
	 * mappings.
	 * 
	 * @param reviewCategoryMap
	 *            existing review->cluster mappings with review IDs between 1
	 *            and <code>maxReviewID</code>
	 * @param maxClusterID
	 *            defines the highest possible cluster ID to be added
	 * @param maxReviewID
	 *            defines the final number of reviews to be achieved
	 */
	private static void addMissingReviews(Map<Long, Integer> reviewCategoryMap, int maxClusterID,
			long maxReviewID) {
		log.info("Adding missing review->cluster assignments. Current number of mappings: "
				+ reviewCategoryMap.size() + ". Checking from review 1 to review " + maxReviewID
				+ ". Adding random clusters between 1 and " + maxClusterID + ".");

		Random rand = new Random();

		for (long reviewID = 1; reviewID <= maxReviewID; reviewID++) {
			if (reviewCategoryMap.containsKey(reviewID))
				continue;

			int clusterID = rand.nextInt(maxClusterID) + 1;
			reviewCategoryMap.put(reviewID, clusterID);

			log.info("\t added missing mapping for review " + reviewID + ": cluster " + clusterID);
			logProgress(reviewID);
		}

		assert reviewCategoryMap.size() == maxClusterID;
		log.info("Finished adding missing review->cluster mappings. New total number of mappings: "
				+ reviewCategoryMap.size());
	}

	private static void scaleClusters(Map<Long, Integer> reviewCategoryMap,
			Map<Integer, Integer> clusterScalingMap) {
		log.info("Scaling clusters in" + reviewCategoryMap.size()
				+ " records with target clusters from 1 to " + clusterScalingMap.size());

		long counter = 0;
		for (long reviewID : reviewCategoryMap.keySet()) {
			counter++;

			int clusterID = reviewCategoryMap.get(reviewID);
			int scaledClusterID = clusterScalingMap.get(clusterID);

			reviewCategoryMap.put(reviewID, scaledClusterID);

			logProgress(counter);
		}

		log.info("finished scaling clusters in " + counter + " records");
	}

	/**
	 * Generates a mapping from the original cluster id to a number between 1
	 * and n with n being the number of different clusters in
	 * <code>reviewCategoryMap</code>.
	 * 
	 * @param reviewCategoryMap
	 *            a mapping from review IDs (key) to cluster IDs (value)
	 * @return a mapping from given cluster IDs to scaled cluster IDs
	 */
	private static Map<Integer, Integer> getClusterScaling(Map<Long, Integer> reviewCategoryMap) {
		log.info("Creating cluster scaling mapping from " + reviewCategoryMap.size() + " records");

		Map<Integer, Integer> mapping = new HashMap<>();

		long counter = 0;
		for (long reviewID : reviewCategoryMap.keySet()) {
			counter++;

			int clusterID = reviewCategoryMap.get(reviewID);

			if (!mapping.containsKey(clusterID))
				mapping.put(clusterID, mapping.size() + 1);

			logProgress(counter);
		}

		log.info("Created cluster scaling mapping for " + mapping.size() + "clusters: \n" + mapping);

		return mapping;
	}

	private static void writeSorted(Map<Long, Integer> reviewCategoryMap, Path path)
			throws IOException {

		log.info("Starting writing " + reviewCategoryMap.size() + " records to " + path);

		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream outStream = fs.create(path);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream));

		for (long reviewID = 1; reviewID <= reviewCategoryMap.size(); reviewID++) {
			int clusterID = reviewCategoryMap.get(reviewID);
			assert clusterID > 0;

			bw.write(clusterID);
			if (reviewID < reviewCategoryMap.size())
				bw.write("\n");

			logProgress(reviewID);
		}

		bw.close();
		log.info("Finished writing. Total output lines: " + reviewCategoryMap.size());
	}

	private static Map<Long, Integer> readClustering(Path kMeansOutputDir) {
		Path pointsPathDir = Path.mergePaths(kMeansOutputDir, new Path("clusteredPoints"));

		log.info("Starting reading review->cluster mappings from " + pointsPathDir);

		Configuration conf = new Configuration();

		SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable> dirIterable;
		dirIterable = new SequenceFileDirIterable<IntWritable, WeightedPropertyVectorWritable>(
				pointsPathDir, PathType.LIST, PathFilters.logsCRCFilter(), conf);

		Map<Long, Integer> reviewCategoryMap = new HashMap<>();

		for (Pair<IntWritable, WeightedPropertyVectorWritable> record : dirIterable) {

			int clusterID = record.getFirst().get();
			long reviewID = getReviewID(record.getSecond().getVector());

			reviewCategoryMap.put(reviewID, clusterID);

			logProgress(reviewCategoryMap.size());
		}

		log.info("Finished reading clusters. total: " + reviewCategoryMap.size());

		return reviewCategoryMap;
	}

	private static void logProgress(long processedRecords) {
		if (processedRecords % 100_000 == 0)
			log.info("\t processed " + processedRecords);
	}

	private static long getReviewID(Vector vector) {
		String vectorID = ((NamedVector) vector).getName();
		String vectorIDNum = vectorID.substring(ToSequenceFileMR.VECTOR_ID_PREFIX.length(),
				vectorID.length());
		return Long.parseLong(vectorIDNum);
	}
}
