package hadoop08.read_clusters;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

public class ClusterDumper {

	private static Logger log = LogManager.getLogger(ClusterDumper.class);

	/**
	 * 
	 * @param args
	 *            <code>clusterDir</code> <code>outputDur</code>
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String inputDir = args[0];
		Path input = new Path(inputDir + "/clusteredPoints/part-m-00000");

		String outputDir = args[1];
		Path output = new Path(outputDir + "/clusterdump.txt");

		Configuration conf = new Configuration();
		FileSystem fs = input.getFileSystem(conf);

		Path qualifiedInput = input.makeQualified(fs.getUri(), fs.getWorkingDirectory());
		SequenceFile.Reader reader = new Reader(conf, Reader.file(qualifiedInput));

		Path qualifiedOutput = output.makeQualified(fs.getUri(), fs.getWorkingDirectory());
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(qualifiedOutput)));

		dump(reader, bw);

		reader.close();
		bw.flush();
		bw.close();
	}

	private static void dump(SequenceFile.Reader reader, BufferedWriter bw) throws IOException {
		IntWritable cluster = new IntWritable();
		WeightedVectorWritable value = new WeightedPropertyVectorWritable();
		long lineNum = 0;

		while (reader.next(cluster, value)) {
			lineNum++;

			NamedVector vector = (NamedVector) value.getVector();
			String vectorName = vector.getName();
			bw.write(vectorName + ", " + cluster.toString() + "\n");

			if (lineNum % 10_000 == 0)
				log.info("wrote " + lineNum + " lines");
		}
	}
}
