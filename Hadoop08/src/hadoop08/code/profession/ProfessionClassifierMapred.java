package hadoop08.code.profession;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
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

import util.HDFSUtils;
import util.StringDoubleList;
import util.StringInteger;
import util.StringIntegerList;

/**
 * This class is used for 3.3 Section B of assignment 2.
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class ProfessionClassifierMapred {

	public static class ProfessionClassifierMapper extends Mapper<Text, Text, Text, Text> {

		private Set<String> wantedPeople;

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException,
				InterruptedException {

			/*
			 * read people to be classified from local cache
			 */
			Path peoplePath = new Path(context.getCacheFiles()[0]);
			List<String> lines = HDFSUtils.readLines(peoplePath, context.getConfiguration());
			wantedPeople = new HashSet<>(lines);
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
			 * TODO this is not needed if we figure out how to separate by " : "
			 * instead of ":" or change the read in files to separate keys from
			 * values by ":" instead of " : "
			 */
			person = new Text(person.toString().trim());
			lemmaCounts = new Text(lemmaCounts.toString().trim());

			if (!wantedPeople.contains(person.toString()))
				return;

			TopProfessions topProf = getTopProfessions(lemmaCounts.toString(), context);

			StringBuilder sb = new StringBuilder();
			for (String prof : topProf.getProfessions())
				sb.append(prof + ", ");

			// remove the last ", "
			sb.delete(sb.length() - 2, sb.length());

			context.write(person, new Text(sb.toString()));
		}

		private TopProfessions getTopProfessions(String lemmaCounts, Context context)
				throws FileNotFoundException, IOException {
			TopProfessions topProf = new TopProfessions();

			/*
			 * read and process the BIG PROFESSION_INDEX_PATH file line by line
			 * 
			 * Hadoop puts all cached files in the working directory of the
			 * slave node, regardless of the original path of the cached file.
			 * Therefore we just need the file's name.
			 */

			Path profPath = new Path(context.getCacheFiles()[1]);
			BufferedReader br = HDFSUtils.getFileReader(profPath, context.getConfiguration());

			String profIndexLine;
			while ((profIndexLine = br.readLine()) != null) {

				String[] parts = profIndexLine.split(" : ");
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
	 * inputPath	HDFS path to the input (directory or file)
	 * outputPath	HDFS path to a not existing directory for the output
	 * peoplePath	HDFS path to a file containing the people to be classified (one per line)
	 * professionIndexPath	HDFS path to the LEMMA_PROFESSION_INDEX file
	 * 
	 * <pre>
	 * @param args
	 *            inputPath outputPath peoplePath professionIndexPath
	 * @throws IllegalArgumentException if <code>args</code> does not contain the four HDFS paths
	 * described above
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 4)
			throw new IllegalArgumentException("Four parameters required. "
					+ "Representing the four HDFS pathes: "
					+ "input, output, people_list, LEMMA_PROFESSION_INDEX");

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

		final Configuration conf = job.getConfiguration();
		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		// required key-value separator is colon instead of tab (default)
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
		conf.set("mapred.textoutputformat.separator", " : ");

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
