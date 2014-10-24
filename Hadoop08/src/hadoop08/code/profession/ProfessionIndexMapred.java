package hadoop08.code.profession;

import hadoop08.util.StringDouble;
import hadoop08.util.StringDoubleList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

public class ProfessionIndexMapred {
	private static final String OUTPUT_KEY_VALUE_SEPARATOR = " : ";

	private static enum Opt {
		minProb, maxProb, maxLemmasPerProf;

		public String asKey() {
			return ProfessionIndexMapred.class.getName() + "." + name();
		}
	}

	public static class ProfessionIndexMapper extends Mapper<Text, Text, Text, StringDouble> {

		private final Log LOG = LogFactory.getLog(getClass());

		/**
		 * Parses value into a StringDouble object. If minProb or maxProb are
		 * set, lemmas with invalid probabilities are skipped
		 */
		@Override
		public void map(Text profession, Text lemmaProb, Context context) throws IOException,
				InterruptedException {

			String[] parts = lemmaProb.toString().split(",");
			String lemma = parts[0];
			Double prob;
			try {
				prob = Double.parseDouble(parts[1]);
			} catch (NumberFormatException e) {
				LOG.error("cannot parse " + lemmaProb.toString() + " into a StringDouble");
				return;
			}

			final Configuration conf = context.getConfiguration();
			double minProb = conf.getDouble(Opt.minProb.asKey(), Double.MIN_VALUE);
			double maxProb = conf.getDouble(Opt.maxProb.asKey(), Double.MAX_VALUE);

			if (prob < minProb || prob > maxProb)
				return;

			if (lemma != null && prob != null)
				context.write(profession, new StringDouble(lemma, prob));
			else
				return;
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
				if (sd != null && sd.getValue() != null)
					list.add(new StringDouble(sd.getString(), sd.getValue()));

			final Configuration conf = context.getConfiguration();
			int maxLemmas = conf.getInt(Opt.maxLemmasPerProf.asKey(), -1);
			if (maxLemmas > 0)
				list = takeTopN(list, maxLemmas);

			StringDoubleList lemmaProbsList = new StringDoubleList(list);
			context.write(profession, lemmaProbsList);
		}

		private List<StringDouble> takeTopN(List<StringDouble> list, int n) {
			Collections.sort(list, new Comparator<StringDouble>() {

				@Override
				public int compare(StringDouble o1, StringDouble o2) {
					// invert order for DESCENDING sorting
					return o2.getValue().compareTo(o1.getValue());
				}
			});

			return list.subList(0, Math.min(list.size(), n));
		}
	}

	/**
	 * Takes in four parameters when called from commandline:
	 * 
	 * <pre>
	 * inputPath	HDFS path to ProfessionLemmaMapred output
	 * 
	 * outputPath	HDFS path to a not existing directory for the output
	 * 
	 * optional	key=value pairs with key in {minProb, maxProb, maxLemmasPerProf},
	 * 		separated by blanks, e.g. <code>minProb=0.1 maxLemmasPerProf=100</code>.
	 * 		Any combination of 1, 2, or 3 of these parameters is accepted.
	 * 
	 * 		<code>minProb</code>: the minimum probability of a lemma given a certain profession 
	 * 		to end up in the index for this profession, e.g. <code>minProb=0.15</code>
	 * 
	 * 		<code>maxProb</code>: the maximum probability of a lemma given a certain profession 
	 * 		to end up in the index for this profession, e.g. <code>maxProb=0.85</code>
	 * 
	 * 		<code>maxLemmasPerProf</code>: sorted by probability how many lemmas should be put
	 * 		in the index of one profession at max, e.g.
	 * <code>maxLemmasPerProf=0100</code>
	 * 
	 * </pre>
	 * 
	 * @param args
	 *            inputPath outputPath (optional)*
	 */
	public static void main(String[] args) throws Exception {

		if (args.length < 2)
			throw new IllegalArgumentException(
					"need at least two parameters: inputPath and outputPath");

		if (args.length > 5)
			throw new IllegalArgumentException("Too many parameters. Can take max 5: "
					+ "inputPath outputPath, minProb maxProb maxLemmasPerProf");

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
		storeOptionalParams(conf, args);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}

	private static void storeOptionalParams(Configuration conf, String[] args) {
		if (args.length < 3)
			return; // no optional parameters

		for (int i = 2; i < args.length; i++) {
			final String[] parts = args[i].split("=");
			Opt key = Opt.valueOf(parts[0]);

			if (key == Opt.maxLemmasPerProf) {
				int value = Integer.parseInt(parts[1]);
				conf.setInt(key.asKey(), value);
			} else {
				double value = Double.parseDouble(parts[1]);
				conf.setDouble(key.asKey(), value);
			}
		}

	}
}
