package code.articles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.WikipediaPageInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 * 
 * @author Hadoop 08 (Steven, Calvin, Paul, Georg)
 * @version 0.2
 * @since 10/2/14
 */
public class GetArticlesMapred {

	public static class GetArticlesMapper extends
			Mapper<LongWritable, WikipediaPage, Text, Text> {
		public static Set<String> peopleArticlesTitles = new HashSet<String>();

		@Override
		protected void setup(
				Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {

			File file = new File("people.txt");
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			// read from people file, add each line to people set
			while ((line = br.readLine()) != null) {
				peopleArticlesTitles.add(line);
			}
			br.close();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage,
				Context context) throws IOException, InterruptedException {

			// input page's title is in our set of wanted articles -> take it
			if (peopleArticlesTitles.contains(inputPage.getTitle())) {
				Text articleXML = new Text(inputPage.getRawXML());
				context.write(new Text(), articleXML);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			URISyntaxException, InterruptedException, ClassNotFoundException {

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

		job.submit();

	}
}
