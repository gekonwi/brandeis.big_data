package hadoop08.code.profession;

import hadoop08.util.HDFSUtils;
import hadoop08.util.StringDouble;
import hadoop08.util.StringInteger;
import hadoop08.util.StringIntegerList;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfessionLemmaMapred {
	private static final String PROFESSIONS_PATH_KEY = ProfessionLemmaMapred.class.getName()
			+ ".PROFESSION_PATH_KEY";
	private static final String PROF_LEMMA_SEPARATOR = " ::: ";

	public static class ProfessionIndexMapper extends Mapper<Text, Text, Text, LongWritable> {

		private Map<String, List<String>> personProfessions;

		@Override
		protected void setup(Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			// allows to set the professions map from unit tests
			if (personProfessions != null)
				return;

			/*
			 * create a HashMap that separates each line from the
			 * profession_train file into a person and list of its professions
			 */

			Path path = new Path(context.getConfiguration().get(PROFESSIONS_PATH_KEY));
			List<String> lines = HDFSUtils.readLines(path, context.getConfiguration());

			personProfessions = new HashMap<String, List<String>>();
			for (String s : lines) {
				String[] parts = s.split(" : ");
				String name = parts[0];
				String profs = parts[1];

				List<String> profsList = Arrays.asList(profs.split(", "));
				personProfessions.put(name, profsList);
			}
		}

		/**
		 * transform the ((key), (value)) pair:
		 * 
		 * <pre>
		 * {@code
		 * ((person1), (<lemma1,freq1>,<lemma2,freq2>,<lemma3,freq3>))
		 * }
		 * </pre>
		 * 
		 * 
		 * with
		 * 
		 * <pre>
		 * {@code
		 * person1 : profession1, profession2, profession3
		 * }
		 * </pre>
		 * 
		 * into profession-lemma pairs like the following ((key), (value))
		 * pairs:
		 * 
		 * <pre>
		 * {@code
		 * ((profession1 ::: lemma1), (1))
		 * ((profession1 ::: lemma2), (1))
		 * ((profession1 ::: lemma3), (1))
		 * ((profession2 ::: lemma1), (1))
		 * ((profession2 ::: lemma2), (1))
		 * ((profession2 ::: lemma3), (1))
		 * ((profession3 ::: lemma1), (1))
		 * ((profession3 ::: lemma2), (1))
		 * ((profession3 ::: lemma3), (1))
		 * }
		 * </pre>
		 */
		@Override
		public void map(Text person, Text lemmaFreqs, Context context) throws IOException,
				InterruptedException {

			/*
			 * make up for the fact that the input key-value pairs are separated
			 * by " : " but the KeyValueLineRecordReader can only separate by
			 * one character (in our case ":")
			 */
			person = new Text(person.toString().trim());
			lemmaFreqs = new Text(lemmaFreqs.toString().trim());

			if (!personProfessions.containsKey(person.toString()))
				return;

			StringIntegerList lemmaFreqList = new StringIntegerList();
			lemmaFreqList.readFromString(lemmaFreqs.toString());

			// For each lemma in the article
			for (StringInteger lemmaFreq : lemmaFreqList.getIndices()) {
				// For each profession associated with the article
				for (String p : personProfessions.get(person.toString())) {
					// profession-lemma as key to allow counting in reducer
					final Text key = new Text(p + PROF_LEMMA_SEPARATOR + lemmaFreq.getString());
					context.write(key, new LongWritable(1));
				}
			}
		}
	}

	public static class ProfessionIndexReducer extends
			Reducer<Text, LongWritable, Text, StringDouble> {

		private static HashMap<String, Integer> professionCounts;

		@Override
		protected void setup(Reducer<Text, LongWritable, Text, StringDouble>.Context context)
				throws IOException, InterruptedException {

			// allows to set the counts from unit tests
			if (professionCounts != null)
				return;
			Path path = new Path(context.getConfiguration().get(PROFESSIONS_PATH_KEY));
			List<String> lines = HDFSUtils.readLines(path, context.getConfiguration());
			professionCounts = ProfessionUtils.getProfessionCounts(lines);
		}

		/**
		 * transform the ((key), (value)) pairs:
		 * 
		 * <pre>
		 * {@code
		 * ((profession1 ::: lemma1), ([1,1,1,1]))
		 * ((profession1 ::: lemma2), ([1,1,1]))
		 * ((profession2 ::: lemma3), ([1]))
		 * }
		 * </pre>
		 * 
		 * with, e.g., 10 people having profession1 and 20 people having
		 * profession2, into ((key), (value)) pairs like:
		 * 
		 * <pre>
		 * {@code
		 * ((profession1), (<lemma1,4.0 / 10.0>))
		 * ((profession1), (<lemma2,3.0 / 10.0>))
		 * ((profession2), (<lemma3,1.0 / 20.0>))
		 * }
		 * </pre>
		 */
		@Override
		public void reduce(Text profLemma, Iterable<LongWritable> counts, Context context)
				throws IOException, InterruptedException {

			long count = 0;
			for (@SuppressWarnings("unused")
			LongWritable c : counts)
				count++;

			String[] parts = profLemma.toString().split(PROF_LEMMA_SEPARATOR);
			String profession = parts[0];
			String lemma = parts[1];

			double prob = ((double) count) / professionCounts.get(profession);

			context.write(new Text(profession), new StringDouble(lemma, prob));
		}
	}

	/**
	 * <code>inputPath</code>: containing the ARTICLE_LEMMA_INDEX <br>
	 * <code>outputPath</code>: a directory for storing the profession-lemma
	 * counts <br>
	 * <code>professionsPath</code>: containing the person -> professions
	 * mapping
	 * 
	 * @param args
	 *            inputPath outputPath professionsPath
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 3)
			throw new IllegalArgumentException("Please provide the following three arguments: "
					+ "inputPath outputPath professionsPath");

		Job job = Job.getInstance(new Configuration());

		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringDouble.class);

		job.setMapperClass(ProfessionIndexMapper.class);
		job.setReducerClass(ProfessionIndexReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Configuration conf = job.getConfiguration();

		final String professionsPath = args[2];
		job.addCacheFile(new URI(professionsPath));
		conf.set(PROFESSIONS_PATH_KEY, professionsPath);

		/*
		 * required key-value separator is " : " instead of tab (default).
		 * However KeyValueLineRecordReader only accepts one separator bit. Our
		 * mapper makes up for it.
		 */
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");

		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		job.setJarByClass(ProfessionLemmaMapred.class);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
