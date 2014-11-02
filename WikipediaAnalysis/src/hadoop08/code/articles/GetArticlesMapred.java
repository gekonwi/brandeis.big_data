package hadoop08.code.articles;

import hadoop08.util.HDFSUtils;
import hadoop08.util.WikipediaPageInputFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * This class is used for Section A of assignment 1. You are supposed to run the
 * code taking the wikipedia dump file as input, and output being the raw XML of
 * Wikipedia articles that matches the people.txt list.
 * 
 * @author Calvin Wang, minwang@brandeis.edu
 */
public class GetArticlesMapred {

	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {

		// used to store people names to match up with Wikipedia articles
		public static Set<String> wantedTitles;

		Log LOG = LogFactory.getLog(GetArticlesMapper.class);

		private static final Path PEOPLE_FILEPATH = new Path("people.txt");

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// read from people file in HDFS, add each line to people set
			final List<String> lines = HDFSUtils.readLines(PEOPLE_FILEPATH,
					context.getConfiguration());
			wantedTitles = new HashSet<>(lines);
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context)
				throws IOException, InterruptedException {

			// input page's title is in our set of wanted articles -> take it
			if (wantedTitles.contains(inputPage.getTitle())) {
				Text articleXML = new Text(inputPage.getRawXML());
				context.write(new Text(), articleXML);
			}
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException,
			InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(GetArticlesMapper.class);

		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(GetArticlesMapred.class);

		// so we don't have to specify the job name when starting job on cluster
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
