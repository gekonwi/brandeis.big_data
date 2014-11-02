package hadoop08.code.profession;

import java.util.HashMap;
import java.util.List;

public class ProfessionUtils {

	/**
	 * Read all lines from a given professions training file in HDFS
	 * 
	 * @param fileLines
	 *            each entry has to have the format:
	 *            <p>
	 *            article name : profession1, profession2, ... <br>
	 *            <p>
	 * @return a HashMap<String, Integer> where the key is a profession and the
	 *         value is the frequency of the profession from the input file
	 */
	public static HashMap<String, Integer> getProfessionCounts(List<String> fileLines) {
		HashMap<String, Integer> result = new HashMap<String, Integer>();
	
		for (String line : fileLines) {
			String[] professions = line.split(" : ")[1].split(", ");
			for (String p : professions)
				if (result.containsKey(p))
					result.put(p, result.get(p) + 1);
				else
					result.put(p, 1);
		}
	
		return result;
	}

}
