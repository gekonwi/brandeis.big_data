package code.lemma;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import code.articles.GetArticlesMapred;
import code.articles.GetArticlesMapred.GetArticlesMapper;
import util.StringIntegerList;
import util.WikipediaPageInputFormat;
import util.StringIntegerList.StringInteger;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
		InterruptedException {
			
		}
	}

	public static class LemmaIndexReducer extends
	Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text article, Iterable<StringInteger> LemmaAndFreqs, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws IOException,
	URISyntaxException, InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LemmaIndexMapper.class);
		job.setReducerClass(LemmaIndexReducer.class);

		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(LemmaIndexMapred.class);

		job.submit();

	}
}
