package code.articles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

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
 * @author Hadoop 08 (Steven, Calvin, Paul, Georg)
 * @version 0.2
 * @since 10/2/14
 */
public class GetArticlesMapred {

	public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
		
		public static List<String> peopleList = new ArrayList<String>(); //used to store people names to match up with Wikipedia articles

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context)
				throws IOException, InterruptedException {

			File file = new File("people.txt");
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			//loop used to add each line from people.txt into peopleList
			while ((line = br.readLine()) != null) {
				peopleList.add(line);
			}
			br.close();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context) 
				throws IOException, InterruptedException {

			//conditional to take out the articles that have titles matching in peopleList
			if(peopleList.contains(inputPage.getTitle())) {
				Text articleXML = new Text(inputPage.getRawXML());
				context.write(new Text(), articleXML);
			}
		}
	}

	public static void main(String[] args) 
			throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(GetArticlesMapper.class);

		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(GetArticlesMapred.class);

		// so we don't have to specify in the argument
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		job.submit();

	}
}
