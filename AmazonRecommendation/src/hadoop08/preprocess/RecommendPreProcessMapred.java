package hadoop08.preprocess;

import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Preprocesses the original input file, filter by provided tags.
 * 
 * @author Calvin Wang
 *
 */
public class RecommendPreProcessMapred {

	public static class RecommendPreProcessMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		}
		
		@Override
		public void map(Text text, Text text2, Context context) throws IOException,
				InterruptedException {

			String line = text.toString() + text2.toString();
			Scanner scan = new Scanner(line);
			String tag;
			if (scan.hasNext()) {
				tag = scan.next();
			
				if (tag.equals("product/productID:") || (tag.equals("review/userId:") || (tag.equals("review/score:")))) {
					Text tLine = new Text(line);
	
					context.write(tLine, new Text());
				}
			}
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

		job.setMapperClass(RecommendPreProcessMapper.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Configuration conf = job.getConfiguration();

		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		job.setJarByClass(RecommendPreProcessMapred.class);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
