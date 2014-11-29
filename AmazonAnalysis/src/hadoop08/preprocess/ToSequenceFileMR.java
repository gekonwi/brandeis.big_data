package hadoop08.preprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * TODO
 * 
 * @author Paul,
 */
public class ToSequenceFileMR {

	public static final String VECTOR_ID_PREFIX = "review_";

	public static class ToSequenceFileMapper extends Mapper<Text, Text, Text, Text> {

		/**
		 * TODO
		 */
		@Override
		public void map(Text lineNum, Text review, Context context) throws IOException,
				InterruptedException {
			Text reviewID = new Text(VECTOR_ID_PREFIX + lineNum.toString());
			context.write(reviewID, review);
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ToSequenceFileMapper.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(ToSequenceFileMR.class);

		// so we don't have to specify the job name when starting job on cluster
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
