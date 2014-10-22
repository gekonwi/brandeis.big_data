package hadoop08.code.profession;

import hadoop08.util.StringDouble;
import hadoop08.util.StringDoubleList;

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

public class ProfessionIndexMapred {
	private static final String OUTPUT_KEY_VALUE_SEPARATOR = " : ";

	public static class ProfessionIndexMapper extends Mapper<Text, Text, Text, StringDouble> {

		/**
		 * Just parses value into a StringDouble object
		 */
		@Override
		public void map(Text profession, Text lemmaProb, Context context) throws IOException,
				InterruptedException {

			String[] parts = lemmaProb.toString().split(",");
			String lemma = parts[0];
			double prob;
			// work-around for null Double -- some words aren't receiving probabilities
			// TODO: fix ^
			if (parts[1].toString() != null) {
				prob = Double.parseDouble(parts[1]);
			} else { return; }
			context.write(profession, new StringDouble(lemma, prob));
		}
	}

	public static class ProfessionIndexReducer extends
			Reducer<Text, StringDouble, Text, StringDoubleList> {

		/**
		 * collects all lemma probabilities (<code>values</code>) associated
		 * with the given profession (<code>key</code>) into a
		 * {@link StringDoubleList} as output value. This results in the
		 * required output format:
		 * 
		 * <pre>
		 * PROFESSION : <LEMMA1,PROB1>,<LEMMA2,PROB2>
		 * </pre>
		 */
		@Override
		public void reduce(Text profession, Iterable<StringDouble> lemmaProbs, Context context)
				throws IOException, InterruptedException {

			List<StringDouble> list = new ArrayList<>();
			for (StringDouble sd : lemmaProbs)
				/*
				 * Haddoop reuses the same StringDouble object in the iterator
				 * and changes the value of it. Thus we have to create a new
				 * StringDouble object for each iteration to preserve the
				 * different values.
				 */
				// another workaround for null lemma and probability values...
				if (sd.getString() != null && sd.getValue() != null) {
					list.add(new StringDouble(sd.getString(), sd.getValue()));
				} else { return; }
			StringDoubleList lemmaProbsList = new StringDoubleList(list);
			context.write(profession, lemmaProbsList);
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());

		// mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringDouble.class);

		// reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringDoubleList.class);

		job.setMapperClass(ProfessionIndexMapper.class);
		job.setReducerClass(ProfessionIndexReducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(ProfessionIndexMapred.class);

		// so we don't have to specify the job name when starting job on cluster
		final Configuration conf = job.getConfiguration();

		conf.set("mapreduce.job.queuename", "hadoop08");
		conf.set("mapred.textoutputformat.separator", OUTPUT_KEY_VALUE_SEPARATOR);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
