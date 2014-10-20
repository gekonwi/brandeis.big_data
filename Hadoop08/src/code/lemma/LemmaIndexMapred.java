package code.lemma;

import hadoop08.util.HDFSUtils;
import hadoop08.util.StringIntegerList;
import hadoop08.util.WikipediaPageInputFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * This class is used for Section C.1 of assignment 1. You are supposed to run
 * the code taking the GetArticlesMapred's output as input, and output being the
 * lemma index.
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends
			Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		private static final Path STOPWORDS_FILEPATH = new Path("stopwords.csv");

		private static final Log LOG = LogFactory.getLog(LemmaIndexMapper.class);

		private Tokenizer tokenizer;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			List<String> lines = HDFSUtils
					.readLines(STOPWORDS_FILEPATH, context.getConfiguration());
			HashSet<String> stopWords = new HashSet<>(lines);
			tokenizer = new Tokenizer(stopWords);
		}

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context)
				throws IOException, InterruptedException {
			String article = ""; // used to store Wikipedia article body

			try {
				// retrieve the body of the Wikipedia article
				article = getArticleBody(page.getRawXML());
			} catch (XMLStreamException e) {
				LOG.error("Failed parsing XML for article: " + page.getTitle(), e);
				return;
			}

			List<String> lemmas = tokenizer.getLemmas(article);
			Map<String, Integer> lemmaCounts = countLemmas(lemmas);
			StringIntegerList lemmaList = new StringIntegerList(lemmaCounts);

			context.write(new Text(page.getTitle()), lemmaList);
		}

		public static Map<String, Integer> countLemmas(List<String> lemmas) {
			Map<String, Integer> map = new HashMap<String, Integer>();

			for (String lemma : lemmas)
				if (map.containsKey(lemma))
					map.put(lemma, map.get(lemma) + 1);
				else
					map.put(lemma, 1);

			return map;
		}

		/**
		 * XML parsing helper method that traverses through an XML'ed Wikipedia
		 * article and looks for the open tag <text>, with this, return the
		 * element text that is encapsulated in the tag
		 * 
		 * @param rawXML
		 *            UTF-8 encoded xml containing the article text
		 * 
		 * @return
		 * @throws UnsupportedEncodingException
		 * @throws XMLStreamException
		 */
		public static String getArticleBody(String rawXML) throws UnsupportedEncodingException,
				XMLStreamException {

			XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
			XMLStreamReader xmlStreamReader = xmlInputFactory
					.createXMLStreamReader(new ByteArrayInputStream(rawXML.getBytes("UTF-8")));

			while (xmlStreamReader.hasNext()) {
				// General STaX technique:

				// check if current event is an open tag
				int event = xmlStreamReader.next();
				if (event != XMLStreamConstants.START_ELEMENT)
					continue;

				// article's tag name must be text
				if (!xmlStreamReader.getLocalName().equals("text"))
					continue;

				if (!xmlStreamReader.isEndElement())
					return xmlStreamReader.getElementText();
			}

			throw new IllegalStateException("no article found in XML");
		}
	}

	private static final String KEY_VALUE_SEPARATOR = " : ";

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {

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

		final Configuration conf = job.getConfiguration();

		// so we don't have to specify the job name when starting job on cluster
		conf.set("mapreduce.job.queuename", "hadoop08");

		// assignment requires " : " instead of the default "\t" as separator
		conf.set("mapred.textoutputformat.separator", KEY_VALUE_SEPARATOR);

		// execute the job with verbose prints
		job.waitForCompletion(true);
	}
}
