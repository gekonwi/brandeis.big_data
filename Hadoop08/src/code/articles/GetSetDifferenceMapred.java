package code.articles;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.HDFSUtils;

public class GetSetDifferenceMapred {
	public static class GetSetDifferenceMapper extends Mapper<Text, Text, Text, Text> {

		// used to store people names to match up with Wikipedia articles
		public static Set<String> titles;

		Log LOG = LogFactory.getLog(GetSetDifferenceMapper.class);

		private static final Path LEMMA_OUTPUT_TITLES = new Path("lemma_index_output_titles.txt");
		private static Integer counter = 0;

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException,
				InterruptedException {
			// read from people file in HDFS, add each line to people set
			final List<String> lines = HDFSUtils.readLines(LEMMA_OUTPUT_TITLES,
					context.getConfiguration());
			titles = new HashSet<>(lines);
		}

		@Override
		public void map(Text articleID, Text text, Context context) throws IOException,
				InterruptedException {
			if (!titles.contains(articleID.toString())) {
				counter++;
				context.write(articleID, new Text(counter.toString()));
			}
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException,
			InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(GetSetDifferenceMapper.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(GetSetDifferenceMapred.class);

		// so we don't have to specify the job name when starting job on cluster
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
