package hadoop08.preprocess;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Preprocesses the original input file, by using the Tokenizer we developed
 * from assignment 1 and 2 to remove stopwords and punctuation and to lemmatize
 * the rest.
 * 
 * The input file is expected to be the output of
 * {@link NumberLines#run(java.nio.file.Path, java.nio.file.Path)}, i.e. each
 * line starts with an integer indicating the line number, followed by a
 * <code>\t</code> character and the actual line content.
 * 
 * The output has the same format, however there is no guarantee about the
 * output order (due to the unpredictable order in which Hadoop MapReduce
 * executes map and reduce jobs). The output has the same amount of lines as the
 * input (including 'blank' lines with only line number and the <code>\t</code>
 * character).
 * 
 * @author Calvin Wang
 * 
 */
public class LemmatizerMR {

	private static final String STOPWORDS_FILEPATH = "stopwords.csv";

	public static class LemmatizerMapper extends Mapper<Text, Text, Text, Text> {

		private HashSet<String> stopWords;
		private Tokenizer tokenizer;

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException,
				InterruptedException {

			// allows to set custom stopWords in unit tests
			if (stopWords == null) {
				// stopwords file is cached on the mapper
				java.nio.file.Path path = Paths.get(STOPWORDS_FILEPATH);
				List<String> lines = Files.readAllLines(path, Charset.forName("UTF-8"));
				stopWords = new HashSet<>(lines);
			}

			tokenizer = new Tokenizer(stopWords);
		}

		/**
		 * 
		 * @param stopWords
		 *            list of unwanted lemmas like <i>I, you, be</i>
		 */
		public void setStopWords(HashSet<String> stopWords) {
			this.stopWords = stopWords;
		}

		@Override
		public void map(Text lineNumText, Text review, Context context) throws IOException,
				InterruptedException {

			String reviewString = review.toString();
			List<String> lemmas = tokenizer.getLemmas(reviewString);

			context.write(lineNumText, new Text(lemmasToString(lemmas)));
		}

		public static String lemmasToString(List<String> list) {
			StringBuilder sb = new StringBuilder();

			for (String s : list)
				sb.append(s + " ");

			// remove the last blank
			if (sb.length() != 0)
				sb.deleteCharAt(sb.length() - 1);

			return sb.toString();
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2)
			throw new IllegalArgumentException("need two parameters: inputPath and outputPath");

		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LemmatizerMapper.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Configuration conf = job.getConfiguration();

		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		job.setJarByClass(LemmatizerMR.class);

		job.addCacheFile(new Path(STOPWORDS_FILEPATH).toUri());

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
