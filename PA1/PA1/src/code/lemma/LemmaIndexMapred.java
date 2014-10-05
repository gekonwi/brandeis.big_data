package code.lemma;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

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
 * This class is used for Section C.1 of assignment 1. You are supposed to run
 * the code taking the GetArticlesMapred's output as input, and output being the
 * lemma index.
 * 
 * @author Steven Hu, stevenhh@brandeis.edu
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends
			Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		// for XML parsing
		private final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		private XMLStreamReader xmlStreamReader;

		private final Tokenizer tokenizer;

		public LemmaIndexMapper() throws FileNotFoundException {
			tokenizer = new Tokenizer();
		}

		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context)
				throws IOException, InterruptedException {
			String article = ""; // used to store Wikipedia article body

			try {
				// retrieve the body of the Wikipedia article
				article = xmlParser(page, xmlInputFactory, xmlStreamReader);
			} catch (XMLStreamException e) {
				e.printStackTrace();
			}

			// run the article body through the Tokenizer and save the lemmas
			// and their count into StringIntegerList
			StringIntegerList lemmaList = new StringIntegerList(tokenizer.tokenize(article));

			context.write(new Text(page.getTitle()), lemmaList);
		}

		/**
		 * XML parsing helper method that traverses through an XML'ed Wikipedia
		 * article and looks for the open tag <text>, with this, return the
		 * element text that is encapsulated in the tag
		 * 
		 * @param page
		 * @param xmlInputFactory
		 * @param xmlStreamReader
		 * @return
		 * @throws UnsupportedEncodingException
		 * @throws XMLStreamException
		 */
		public static String xmlParser(WikipediaPage page, XMLInputFactory xmlInputFactory,
				XMLStreamReader xmlStreamReader) throws UnsupportedEncodingException,
				XMLStreamException {
			String article = "";

			xmlStreamReader = xmlInputFactory.createXMLStreamReader(new ByteArrayInputStream(page
					.getRawXML().getBytes("UTF-8")));
			int event = xmlStreamReader.getEventType();
			boolean hasNext = true;

			while (hasNext) {
				// Using general STaX technique where the current event is
				// checked if it is a open tag
				switch (event) {
				case XMLStreamConstants.START_ELEMENT:
					// text is the tag name for the article body
					if (xmlStreamReader.getLocalName().equals("text")) {
						if (!xmlStreamReader.isEndElement())
							// gets text of an open tag
							article = xmlStreamReader.getElementText();
						hasNext = false;
					}
				}
				if (xmlStreamReader.hasNext())
					event = xmlStreamReader.next();
				else
					break;
			}

			return article;
		}
	}

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

		// so we don't have to specify the job name when starting job on cluster
		job.getConfiguration().set("mapreduce.job.queuename", "hadoop08");

		job.submit();

	}
}
