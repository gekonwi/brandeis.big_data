package code.lemma;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.StringIntegerList;
import util.WikipediaPageInputFormat;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * @author Hadoop 08 (Steven, Calvin, Paul, Georg)
 * @version 0.2
 * @since 10/2/14
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends
			Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		private final XMLInputFactory xmlInputFactory = XMLInputFactory
				.newInstance(); // for XML parsing
		private XMLStreamReader xmlStreamReader; // for XML parsing
		private final Tokenizer tokenizer = new Tokenizer();

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context)
				throws IOException, InterruptedException {
			String article = ""; // used to store Wikipedia article body

			try {
				xmlStreamReader = xmlInputFactory
						.createXMLStreamReader(new ByteArrayInputStream(page
								.getRawXML().getBytes("UTF-8")));
				int event = xmlStreamReader.getEventType();
				while (true) {
					// Using general STaX technique where the current event is
					// checked if it is a open tag
					switch (event) {
					case XMLStreamConstants.START_ELEMENT:
						// text is the tag name for the article body
						if (xmlStreamReader.getLocalName().equals("text")) {
							if (!xmlStreamReader.isEndElement())
								// gets text of an open tag
								article = xmlStreamReader.getElementText();
						}
					}
					if (xmlStreamReader.hasNext())
						event = xmlStreamReader.next();
					else
						break;
				}
			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

			// run the article body through the Tokenizer and save the lemmas
			// and their count into StringIntegerList
			StringIntegerList lemmaList = new StringIntegerList(
					tokenizer.tokenize(article));

			context.write(new Text(page.getTitle()), lemmaList);
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		// Job configs
		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringIntegerList.class);

		job.setMapperClass(LemmaIndexMapper.class);

		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(LemmaIndexMapred.class);

		// so we don't have to specify the job name when starting job on cluster
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		job.submit();

	}
}
