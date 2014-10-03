package code.inverted;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;
import code.articles.GetArticlesMapred;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends
			Mapper<Text, Text, Text, StringInteger> {

		@Override
		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement inverted index mapper here
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma,
				Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// context.wr
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringInteger.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setInputFormatClass(LemmaIndexInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(GetArticlesMapred.class);

		job.submit();
	}
}
