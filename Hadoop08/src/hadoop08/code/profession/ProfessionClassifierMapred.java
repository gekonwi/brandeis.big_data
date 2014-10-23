package hadoop08.code.profession;

import hadoop08.util.HDFSUtils;
import hadoop08.util.StringDoubleList;
import hadoop08.util.StringInteger;
import hadoop08.util.StringIntegerList;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class is used for 3.3 Section B of assignment 2.
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class ProfessionClassifierMapred {

	/*
	 * required key-value separator is " : " instead of tab (default). However
	 * KeyValueLineRecordReader only accepts one separator bit. Our mapper makes
	 * up for it.
	 */
	private static final String INPUT_KEY_VALUE_SEP = ":";
	private static final String OUTPUT_KEY_VALUE_SEP = " : ";
	private static final String OUTPUT_PROFESSIONS_SEP = ", ";

	private static final String FLAG_CACHE_PROBS = "-cacheProbs";
	public static final String CONF_KEY_CACHE_PROBS = ProfessionClassifierMapred.class.getName()
			+ ".cacheProbs";

	public static class ProfessionClassifierMapper extends Mapper<Text, Text, Text, Text> {

		private Set<String> wantedPeople;
		private Map<String, Map<String, Double>> probs;

		// private static Logger LOG = LogManager.getLogger("Main");

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException,
				InterruptedException {

			// allows setting custom people set and profession-lemma
			// probabilities from unit tests

			if (wantedPeople == null)
				wantedPeople = readWantedPeople(context);

			boolean cacheProbs = context.getConfiguration().getBoolean(CONF_KEY_CACHE_PROBS, false);
			if (probs == null && cacheProbs)
				probs = readLemmaProfessionProbs(context);
		}

		private Map<String, Map<String, Double>> readLemmaProfessionProbs(Context context)
				throws IOException {
			Map<String, Map<String, Double>> result = new HashMap<>();

			Path profPath = new Path(context.getCacheFiles()[1]);
			BufferedReader br = HDFSUtils.getFileReader(profPath, context.getConfiguration());

			String profIndexLine;
			while ((profIndexLine = br.readLine()) != null) {

				String[] parts = profIndexLine.split(INPUT_KEY_VALUE_SEP);
				String profession = parts[0];
				String lemmaProbs = parts[1];

				StringDoubleList lemmaProbsList = new StringDoubleList();
				lemmaProbsList.readFromString(lemmaProbs);
				Map<String, Double> lemmaProbsMap = lemmaProbsList.getMap();

				result.put(profession, lemmaProbsMap);
			}

			br.close();

			return result;
		}

		/**
		 * read people to be classified from local cache
		 * 
		 * @return
		 */
		private HashSet<String> readWantedPeople(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException {
			Path peoplePath = new Path(context.getCacheFiles()[0]);
			List<String> lines = HDFSUtils.readLines(peoplePath, context.getConfiguration());
			return new HashSet<>(lines);
		}

		public void setWantedPeople(HashSet<String> wantedPeople) {
			this.wantedPeople = wantedPeople;
		}

		/**
		 * transforms:
		 * 
		 * <pre>
		 * {@code
		 * (person1, <lemma1,freq1>,<lemma2,freq2>,<lemma3,freq3>)
		 * }
		 * </pre>
		 * 
		 * into up to three profession assignments with corresponding
		 * probability:
		 * 
		 * <pre>
		 * {@code
		 * (person1, <profession1, profession2, profession3>)
		 * }
		 * </pre>
		 * 
		 * These professions are the most likely ones for the given
		 * <code>person1</code>.
		 * 
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void map(Text person, Text lemmaCounts, Context context) throws IOException,
				InterruptedException {
			/*
			 * make up for the fact that the input key-value pairs are separated
			 * by " : " but the KeyValueLineRecordReader can only separate by
			 * one character (in our case ":")
			 */
			person = new Text(person.toString().trim());
			lemmaCounts = new Text(lemmaCounts.toString().trim());

			if (!wantedPeople.contains(person.toString()))
				return;

			// LOG.info("processing wanted person: " + person);

			TopProfessions topProfs;
			if (probs == null)
				topProfs = getTopProfessionsFromFile(lemmaCounts.toString(), context);
			else
				topProfs = getTopProfessionsFromMap(lemmaCounts.toString());

			StringBuilder sb = new StringBuilder();
			for (String prof : topProfs.getProfessions())
				sb.append(prof + OUTPUT_PROFESSIONS_SEP);

			// remove the last ", "
			sb.delete(sb.length() - 2, sb.length());
			Text profs = new Text(sb.toString());

			context.write(person, profs);
			// LOG.info("done. professions: " + profs);
		}

		private TopProfessions getTopProfessionsFromMap(String lemmaCounts) throws IOException {
			TopProfessions topProf = new TopProfessions();

			for (String job : probs.keySet()) {
				double prob = getProfessionProbability(lemmaCounts, probs.get(job));
				topProf.check(job, prob);
			}

			return topProf;
		}

		private TopProfessions getTopProfessionsFromFile(String lemmaCounts, Context context)
				throws FileNotFoundException, IOException {
			TopProfessions topProf = new TopProfessions();

			/*
			 * read and process the BIG PROFESSION_INDEX_PATH file line by line
			 */

			Path profPath = new Path(context.getCacheFiles()[1]);
			BufferedReader br = HDFSUtils.getFileReader(profPath, context.getConfiguration());

			String profIndexLine;
			while ((profIndexLine = br.readLine()) != null) {

				String[] parts = profIndexLine.split(INPUT_KEY_VALUE_SEP);
				String profession = parts[0];
				String lemmaProbs = parts[1];

				double probability = getProfessionProbability(lemmaProbs, lemmaCounts);
				topProf.check(profession, probability);
			}

			br.close();

			return topProf;
		}

		private double getProfessionProbability(String lemmaProbs, String lemmaCounts)
				throws IOException {
			StringDoubleList lemmaProbsList = new StringDoubleList();
			lemmaProbsList.readFromString(lemmaProbs);
			Map<String, Double> lemmaProbsMap = lemmaProbsList.getMap();

			return getProfessionProbability(lemmaCounts, lemmaProbsMap);
		}

		private double getProfessionProbability(String lemmaCounts,
				Map<String, Double> lemmaProbsMap) throws IOException {
			StringIntegerList lemmaCountsList = new StringIntegerList();
			lemmaCountsList.readFromString(lemmaCounts.toString());

			double profProb = 0;

			for (StringInteger lemmaFreq : lemmaCountsList.getIndices()) {
				if (!lemmaProbsMap.containsKey(lemmaFreq.getString()))
					continue;
				/*
				 * we add 1 to each probability because log(x) is negative for x
				 * < 1 and log(1.0) = 0. If a lemma x appears in all articles of
				 * people with profession p, meaning P(lemma = x | profession =
				 * p) == 1.0, we don't want to ignore this important lemma in
				 * our sum by having log(P(lemma = x | profession = p)) == 0.
				 */
				double lemmaProb = lemmaProbsMap.get(lemmaFreq.getString()) + 1;
				profProb += lemmaFreq.getValue() * Math.log(lemmaProb);
			}

			return profProb;
		}
	}

	/**
	 * Takes in four parameters when called from commandline:
	 * 
	 * <pre>
	 * inputPath	HDFS path to the ARTICLE_LEMMA_INDEX file
	 * 
	 * outputPath	HDFS path to a not existing directory for the output
	 * 
	 * peoplePath	HDFS path to a file containing the people to be classified (one per line)
	 * 
	 * professionIndexPath	HDFS path to the PROFESSION_LEMMA_INDEX file
	 * 
	 * cacheProbs (optional)	set by <code>-cacheProbs</code>.
	 * 		Tells the mapper to store the PROFESSION_LEMMA_INDEX with a fast accessible 
	 * 		data structure in working memory. If not provided, mapper will parse 
	 * 		<code>professionIndexPath</code> on every <code>map()</code>
	 * call.
	 * 
	 * <pre>
	 * @param args
	 *            inputPath outputPath peoplePath professionIndexPath (-cacheProbs)
	 * @throws IllegalArgumentException if <code>args</code> does not contain the four HDFS paths
	 * described above
	 */
	public static void main(String[] args) throws Exception {
		validateParameters(args);

		Job job = Job.getInstance(new Configuration());

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(ProfessionClassifierMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		HDFSUtils.addCacheFile(job, args[2]);
		HDFSUtils.addCacheFile(job, args[3]);

		job.setJarByClass(ProfessionClassifierMapred.class);

		setConfigurationValues(args, job);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}

	private static void setConfigurationValues(String[] args, Job job) {
		Configuration conf = job.getConfiguration();
		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				INPUT_KEY_VALUE_SEP);
		conf.set("mapred.textoutputformat.separator", OUTPUT_KEY_VALUE_SEP);

		if (args.length == 5 && args[4].equals(FLAG_CACHE_PROBS))
			conf.setBoolean(CONF_KEY_CACHE_PROBS, true);
	}

	private static void validateParameters(String[] args) {
		if (args.length < 4 || args.length > 5)
			throw new IllegalArgumentException("Four parameters required. "
					+ "The four HDFS pathes: "
					+ "input output people_list PROFESSION_LEMMA_INDEX. Optionally the flag "
					+ FLAG_CACHE_PROBS + " as fifth");

		if (args.length == 5 && !args[4].equals(FLAG_CACHE_PROBS))
			throw new IllegalArgumentException("The fifth parameter can only be the flag "
					+ FLAG_CACHE_PROBS);
	}
}
