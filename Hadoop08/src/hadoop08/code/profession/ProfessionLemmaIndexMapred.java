package hadoop08.code.profession;

import hadoop08.util.HDFSUtils;
import hadoop08.util.StringInteger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class ProfessionLemmaIndexMapred {

	public static class ProfessionLemmaIndexMapper extends
			Mapper<Text, StringInteger, Text, StringInteger> {

		// set of titles that will be used for lookup in mapper
		public static Set<String> professionTitles;

		Log LOG = LogFactory.getLog(ProfessionLemmaIndexMapper.class);

		private static final Path PROFESSION_TEST_FILEPATH = new Path("profession_test.txt");

		@Override
		protected void setup(Mapper<Text, StringInteger, Text, StringInteger>.Context context)
				throws IOException, InterruptedException {
			final List<String> lines = HDFSUtils.readLines(PROFESSION_TEST_FILEPATH,
					context.getConfiguration());
			professionTitles = new HashSet<>(lines);
		}

		@Override
		public void map(Text articleId, StringInteger lemmaAndFreq, Context context)
				throws IOException, InterruptedException {
			if (professionTitles.contains(articleId.toString()))
				context.write(articleId, lemmaAndFreq);
		}
	}

	/**
	 * 
	 * @param args arg0: lemma_index_output, arg1: output path
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException, URISyntaxException,
			InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringInteger.class);

		job.setMapperClass(ProfessionLemmaIndexMapper.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(ProfessionLemmaIndexMapred.class);

		// so we don't have to specify the job name when starting job on cluster
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
