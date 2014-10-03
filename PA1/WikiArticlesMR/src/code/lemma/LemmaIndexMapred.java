package code.lemma;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.StringIntegerList;
import util.WikipediaPageInputFormat;
import util.StringIntegerList.StringInteger;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * 
 *
 */
public class LemmaIndexMapred {
	public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {

		private XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
		private XMLStreamReader xmlStreamReader;
		private Tokenizer tokenizer= new Tokenizer();
		
		@Override
		public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException,
		InterruptedException{
			String article = "";
			
			try {
				xmlStreamReader = xmlInputFactory.createXMLStreamReader(new ByteArrayInputStream(page.getRawXML().getBytes("UTF-8")));
				int event = xmlStreamReader.getEventType();
				while(true){
					switch(event) {
	                case XMLStreamConstants.START_ELEMENT:
	                	if(xmlStreamReader.getLocalName().equals("text")){
	                		xmlStreamReader.next();
	            			if(!xmlStreamReader.isEndElement())
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
			
			StringIntegerList lemmaList = new StringIntegerList(tokenizer.tokenize(article));
			
			context.write(new Text(lemmaList.toString()), lemmaList);
		}
	}

	public static class LemmaIndexReducer extends
	Reducer<Text, StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text article, Iterable<StringInteger> LemmaAndFreqs, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LemmaIndexMapper.class);
		job.setReducerClass(LemmaIndexReducer.class);

		job.setInputFormatClass(WikipediaPageInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(LemmaIndexMapred.class);

		job.submit();

	}
}
