package code.inverted;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 */
public class InvertedIndexMapred {

	private static final Log LOG = LogFactory.getLog(InvertedIndexMapred.class);

	public static class InvertedIndexMapper extends
			Mapper<Text, Text, Text, StringInteger> {

		@Override
		public void map(Text articleId, Text indices, Context context)
				throws IOException, InterruptedException {
			StringIntegerList siList = new StringIntegerList();
			siList.readFromString(indices.toString());

			String articleIdString = articleId.toString();
			for (StringInteger lemmaFreq : siList.getIndices()) {
				StringInteger articleFreq = new StringInteger(articleIdString,
						lemmaFreq.getValue());
				context.write(new Text(lemmaFreq.getString()), articleFreq);
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma,
				Iterable<StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {

			LOG.info("===== Iterable for lemma [" + lemma + "]: "
					+ articlesAndFreqs.getClass() + "; " + articlesAndFreqs);

			List<StringInteger> siList = new ArrayList<>();

			for (StringInteger si : articlesAndFreqs)
				siList.add(si);

			LOG.info("===== reduced lemma [" + lemma + "]: " + siList);

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
		// job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(InvertedIndexMapred.class);

		job.submit();
	}
}
