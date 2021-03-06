package hadoop08.code.inverted;

import hadoop08.util.StringInteger;
import hadoop08.util.StringIntegerList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class InvertedIndexMapred {

	public static final String KEY_VALUE_SEPARATOR = " : ";

	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, StringInteger> {

		/**
		 * transforms:
		 * 
		 * <pre>
		 * {@code
		 * (article_id1, <lemma1,freq1>,<lemma2,freq2>,<lemma3,freq3>)
		 * }
		 * </pre>
		 * <p>
		 * into:
		 * 
		 * <pre>
		 * {@code lemma1 <article_id1,freq1>
		 * (lemma2, <article_id1,freq2>)
		 * (lemma3, <article_id1,freq3>)
		 * }
		 * </pre>
		 */
		@Override
		public void map(Text articleId, Text indices, Context context) throws IOException,
				InterruptedException {

			// blanks result from ":" instead of " : " as key-value separator
			articleId = new Text(articleId.toString().trim());
			indices = new Text(indices.toString().trim());

			StringIntegerList siList = new StringIntegerList();
			siList.readFromString(indices.toString());

			String articleIdString = articleId.toString();
			for (StringInteger lemmaFreq : siList.getIndices()) {
				Text lemma = new Text(lemmaFreq.getString());
				StringInteger articleFreq = new StringInteger(articleIdString, lemmaFreq.getValue());
				context.write(lemma, articleFreq);
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {

		/**
		 * transforms:
		 * 
		 * <pre>
		 * {@code
		 * (lemma1, <article_id1,freq1>)
		 * (lemma1, <article_id2,freq2>)
		 * (lemma2, <article_id1,freq3>)
		 * }
		 * </pre>
		 * <p>
		 * into:
		 * 
		 * <pre>
		 * {@code
		 * lemma1 : <article_id1,freq1>,<article_id2,freq2>
		 * lemma2 : <article_id1,freq3>
		 * }
		 * </pre>
		 */
		@Override
		public void reduce(Text lemma, Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {

			List<StringInteger> siList = new ArrayList<>();

			/*
			 * here we need to create duplicate instances because the Hadoop
			 * Iterable implementation keeps reference to THE SAME Writable
			 * object between the Iterator next() calls. It just replaces the
			 * object contents. Thus, without duplicating, we would add the same
			 * object multiple times to the list, in the end containing the data
			 * from the last iteration.
			 */
			for (StringInteger si : articlesAndFreqs)
				siList.add(new StringInteger(si.getString(), si.getValue()));

			context.write(lemma, new StringIntegerList(siList));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringInteger.class);

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(InvertedIndexMapred.class);

		final Configuration conf = job.getConfiguration();

		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		// unfortunately this only takes one character as separator so we can't
		// use " : ". The mapper makes up for it.
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
		conf.set("mapred.textoutputformat.separator", KEY_VALUE_SEPARATOR);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
