package hadoop08.code.profession;

import hadoop08.utils.HDFSUtils;
import hadoop08.utils.StringDouble;
import hadoop08.utils.StringDoubleList;
import hadoop08.utils.StringInteger;
import hadoop08.utils.StringIntegerList;

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

	public static class ProfessionIndexMapper extends Mapper<Text, Text, Text, Text> {

		private static final Path PROFESSION_FILEPATH = new Path("profession_train.txt");
		private List<String> wantedPeople;
		private Map<String, List<String>> peopleProfessions;

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			// read from people_train file in HDFS, add each line to
			// peopleProfessions map.
			wantedPeople = HDFSUtils.readLines(PROFESSION_FILEPATH, context.getConfiguration());
			// create a HashMap that separates each line into a person and its
			// professions.
			peopleProfessions = new HashMap<String, List<String>>();
			for (String s : wantedPeople) {
				// Assuming that there is no whitespace in each line
				// (name:prof1,prof2,prof3)
				String name = s.substring(0, s.indexOf(":")).trim();
				String prof = s.substring(s.indexOf(":") + 1).trim();
				// Chose to implement a list because of
				// http://stackoverflow.com/questions/7488643/java-how-to-convert-comma-separated-string-to-arraylist
				List<String> items = Arrays.asList(prof.split("\\s*,\\s*"));
				peopleProfessions.put(name, items);
			}
		}

		@Override
		public void map(Text articleId, Text indices, Context context) throws IOException,
				InterruptedException {
			/*
			 * transform:
			 * 
			 * article_id1 <lemma1,freq1>,<lemma2,freq2>,<lemma3,freq3>
			 * 
			 * into tuples:
			 * 
			 * [profession1, <lemma1,freq1>,<lemma2, freq2>,<lemma3,freq3>]
			 * 
			 * [profession2, <lemma1,freq1>,<lemma2,freq2>,<lemma3,freq3>]
			 * 
			 * [profession3, <lemma1,freq1>,<lemma2,freq2>,<lemma3,freq3>]
			 */

			StringIntegerList siList = new StringIntegerList();
			siList.readFromString(indices.toString());

			String articleIdString = articleId.toString();
			// For each lemma in the article
			for (StringInteger lemmaFreq : siList.getIndices()) {
				// For each profession associated with the article
				if (peopleProfessions.containsKey(articleIdString)) {
					for (String s : peopleProfessions.get(articleIdString)) {
						// Write the profession, all LemmaFreqs associated with that
						// profession
						context.write(new Text(s), new Text(lemmaFreq.getString()));
					}
				}
			}
		}
	}

	public static class ProfessionIndexReducer extends
			Reducer<Text, Text, Text, StringDoubleList> {

		public static HashMap<String, Integer> professionsCount;
		private static final Path PROFESSION_FILEPATH = new Path("profession_train.txt");

		@Override
		protected void setup(Reducer<Text, Text, Text, StringDoubleList>.Context context)
				throws IOException, InterruptedException {
			professionsCount = ProfessionUtils.getProfessionCounts(HDFSUtils.readLines
												(PROFESSION_FILEPATH, context.getConfiguration()));
		}

		@Override
		public void reduce(Text profession, Iterable<Text> lemmas, Context context)
				throws IOException, InterruptedException {

			/*
			 * transform:
			 * 
			 * [profession1, <lemma1,freq1>,<lemma2,freq2>,<lemma1,freq3>]
			 * 
			 * [profession2, <lemma1,freq1>,<lemma2,freq2>,<lemma1,freq3>]
			 * 
			 * [profession3, <lemma1,freq1>,<lemma2,freq2>,<lemma1,freq3>]
			 * 
			 * into:
			 * 
			 * [profession1, <lemma1,freq1 + freq3,prob1>,<lemma2,freq2,prob2>]
			 * 
			 * [profession2, <lemma1,freq1 + freq3,prob1>,<lemma2,freq2,prob2>]
			 * 
			 * [profession3, <lemma1,freq1 + freq3,prob1>,<lemma2,freq2,prob2>]
			 */

			Map<String, Double> lemmaProbs = new HashMap<>();
			double probabilityAdd = 1.0 / professionsCount.get(profession.toString());

			for (Text si : lemmas) {

				if (!lemmaProbs.containsKey(si.toString()))
					lemmaProbs.put(si.toString(), probabilityAdd);
				else
					lemmaProbs.put(si.toString(), lemmaProbs.get(si.toString()) + probabilityAdd);

			}

			StringDoubleList sdl_lemmaProbs = new StringDoubleList(lemmaProbs);
			context.write(profession, sdl_lemmaProbs);
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringInteger.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringDouble.class);

		job.setMapperClass(ProfessionIndexMapper.class);
		job.setReducerClass(ProfessionIndexReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(ProfessionIndexMapred.class);

		// so we don't have to specify the job name when starting job on cluster
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
