package hadoop08.read_clusters;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class ClusterDumper2Proto {
	private static Logger log = LogManager.getLogger(ClusterDumper2Proto.class);

	public static void main(String[] args) {
		Path input = new Path(args[0]);
		int printFirstN = Integer.parseInt(args[1]);

		Map<Integer, List<WeightedPropertyVectorWritable>> points;
		points = ClusterDumper.readPoints(input, printFirstN, new Configuration());

		int count = 0;
		for (Integer key : points.keySet()) {
			if (count > printFirstN)
				break;

			count++;
			String message = "key :" + key;
			message += ", value: " + points.get(key);
			log.info(message);
		}
	}
}
