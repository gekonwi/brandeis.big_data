package hadoop08.preprocess;

import hadoop08.preprocess.Tokenizer;
import hadoop08.utils.HDFSUtils;

import java.io.*;
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
 * Preprocesses the original input file, by using the Tokenizer we developed from
 * assignment 1 and 2 to remove stopwords and punctuation and to lemmatize the rest.
 * 
 * @author Calvin Wang
 *
 */
public class PreProcessMapred {

	private static final String STOPWORDS_FILEPATH = "stopwords.csv";

	public static class PreProcessMapper extends Mapper<Text, Text, Text, Text> {

		private HashSet<String> stopWords;
		private Tokenizer tokenizer;

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// allows to set custom stopWords in unit tests
			if (stopWords == null) {
				Path path = new Path(STOPWORDS_FILEPATH);
				List<String> lines = HDFSUtils.readLines(path, context.getConfiguration());
				stopWords = new HashSet<>(lines);
			}
			tokenizer = new Tokenizer(stopWords);
		}

		/**
		 * Has to be called before running MR unit tests. Otherwise the mapper
		 * will try to retrieve the stop words from an HDFS file.
		 * 
		 * @param stopWords
		 *            list of unwanted lemmas like <i>I, you, be</i>
		 */
		public void setStopWords(HashSet<String> stopWords) {
			this.stopWords = stopWords;
		}
		
		@Override
		public void map(Text text, Text review, Context context) throws IOException,
				InterruptedException {

			String line = text.toString() + review.toString();
			
			context.write(new Text(lemmasToString(tokenizer.getLemmas(line))), new Text());
		}
		
		public static String lemmasToString(List<String> list) {
			String result = "";
			for (String s : list) 
				result = result + " " + s;
			return result;
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2)
			throw new IllegalArgumentException(
					"need two parameters: inputPath and outputPath");
		
		Job job = Job.getInstance(new Configuration());

		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(PreProcessMapper.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Configuration conf = job.getConfiguration();

		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		job.setJarByClass(PreProcessMapred.class);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
