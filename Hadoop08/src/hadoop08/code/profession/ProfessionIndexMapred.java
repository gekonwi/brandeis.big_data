package hadoop08.code.profession;

import hadoop08.util.HDFSUtils;
import hadoop08.util.StringDoubleList;
import hadoop08.util.StringInteger;
import hadoop08.util.StringIntegerList;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProfessionIndexMapred {
	private static final String KEY_VALUE_SEPARATOR = " : ";

	public static class ProfessionIndexMapper extends Mapper<Text, Text, Text, StringInteger> {

		private static final Path PROFESSION_FILEPATH = new Path("profession_train.txt");
		private Map<String, List<String>> peopleProfessions;

		@Override
		protected void setup(Mapper<Text, Text, Text, StringInteger>.Context context)
				throws IOException, InterruptedException {
			/*
			 * create a HashMap that separates each line from the
			 * profession_train file into a person and list of its professions
			 */

			List<String> lines = HDFSUtils.readLines(PROFESSION_FILEPATH,
					context.getConfiguration());

			peopleProfessions = new HashMap<String, List<String>>();
			for (String s : lines) {
				String[] parts = s.split(" : ");
				String name = parts[0];
				String profs = parts[1];

				List<String> profsList = Arrays.asList(profs.split(", "));
				peopleProfessions.put(name, profsList);
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
		 * into the Cartesian product of professions and lemmas like the
		 * following ((key), (value)) pairs:
		 * 
		 * <pre>
		 * {@code
		 * ((profession1), (<lemma1,freq1>))
		 * ((profession1), (<lemma2,freq2>))
		 * ((profession1), (<lemma3,freq3>))
		 * ((profession2), (<lemma1,freq1>))
		 * ((profession2), (<lemma2,freq2>))
		 * ((profession2), (<lemma3,freq3>))
		 * ((profession3), (<lemma1,freq1>))
		 * ((profession3), (<lemma2,freq2>))
		 * ((profession3), (<lemma3,freq3>))
		 * }
		 * </pre>
		 */
		@Override
		public void map(Text person, Text lemmaFreqs, Context context) throws IOException,
				InterruptedException {

			StringIntegerList lemmaFreqList = new StringIntegerList();
			lemmaFreqList.readFromString(lemmaFreqs.toString());

			// For each lemma in the article
			for (StringInteger lemmaFreq : lemmaFreqList.getIndices()) {
				// For each profession associated with the article
				for (String p : peopleProfessions.get(person.toString())) {
					// Write the profession, all LemmaFreqs associated with that
					// profession
					context.write(new Text(p), lemmaFreq);
				}
			}
		}
	}

	public static class ProfessionIndexReducer extends
			Reducer<Text, StringInteger, Text, StringDoubleList> {

		public static HashMap<String, Integer> professionsCount;
		private static final Path PROFESSION_FILEPATH = new Path("profession_train.txt");

		@Override
		protected void setup(Reducer<Text, StringInteger, Text, StringDoubleList>.Context context)
				throws IOException, InterruptedException {
			final List<String> lines = HDFSUtils.readLines(PROFESSION_FILEPATH,
					context.getConfiguration());
			professionsCount = ProfessionUtils.getProfessionCounts(lines);
		}

		/**
		 * transform the ((key), (value)) pair:
		 * 
		 * <pre>
		 * {@code
		 * ((profession1), (<lemma1,freq1>,<lemma2,freq2>,<lemma1,freq3>))
		 * }
		 * </pre>
		 * 
		 * into a ((key), (value)) pair like:
		 * 
		 * <pre>
		 * {@code
		 * ((profession1), (<lemma1,probability1>,<lemma2,probability2>))
		 * }
		 * </pre>
		 */
		@Override
		public void reduce(Text profession, Iterable<StringInteger> lemmasAndFreqs, Context context)
				throws IOException, InterruptedException {

			Map<String, Double> lemmaProbs = new HashMap<>();
			double probabilityAdd = 1.0 / professionsCount.get(profession.toString());

			for (StringInteger si : lemmasAndFreqs) {

				if (!lemmaProbs.containsKey(si.getString()))
					lemmaProbs.put(si.getString(), probabilityAdd);
				else
					lemmaProbs.put(si.getString(), lemmaProbs.get(si.getString()) + probabilityAdd);

			}

			StringDoubleList lemmaProbsList = new StringDoubleList(lemmaProbs);
			context.write(profession, lemmaProbsList);
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());

		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringInteger.class);

		// reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringDoubleList.class);

		job.setMapperClass(ProfessionIndexMapper.class);
		job.setReducerClass(ProfessionIndexReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		/*
		 * TODO in previous assignment we kept the default \t (tab) key value
		 * separator for our output. thus our current ARTICLE_LEMMA_INDEX output
		 * file has a \t between key and value in each line. Having this and
		 * using the KeyValueTextInputFormat as we do here with the default
		 * key-value separator bit (which is \t) works fine for now. As soon as
		 * we run the LemmaIndexMapred again with all the tunings we did since
		 * the last run and the added stop words, we will get " : " between key
		 * and value in each output line as we changed LemmaIndexMapred to do so
		 * according to the assignment and the expected TA test input. Then we
		 * will have to deal with this input as done in
		 * ProfessionClassifierMapred
		 */

		final Configuration conf = job.getConfiguration();
		conf.set("mapred.textoutputformat.separator", KEY_VALUE_SEPARATOR);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(ProfessionIndexMapred.class);

		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
