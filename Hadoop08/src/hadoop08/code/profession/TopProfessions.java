package hadoop08.code.profession;

import hadoop08.util.StringDouble;

import java.util.LinkedList;

/**
 * Used to hold the three most likely professions. New professions can be added
 * by comparing their probabilities with the already added professions. If the
 * one under test is more likely then one of the currently contained, the former
 * replaces the latter.
 * <p>
 * The class makes sure that the three most likely professions are always in
 * order from most to least likely.
 * 
 * @author Georg Konwisser, gekonwi@brandeis.edu
 * 
 */
public class TopProfessions {
	private final LinkedList<StringDouble> professions = new LinkedList<>();
	private static final int MAX_PROFESSIONS_COUNT = 5;

	/**
	 * Check if the given profession is more likely than one of the previously
	 * stored. If so, replace one of the stored by the provided one.
	 * 
	 * @param profession
	 *            the profession to be added if its <code>probability</code> is
	 *            higher than the probability of one of the previously stored
	 *            professions.
	 * @param probability
	 *            probability of <code>profession</code>
	 */
	public void check(String profession, double probability) {
		final StringDouble sd = new StringDouble(profession, probability);

		boolean added = false;

		for (int i = 0; i < professions.size(); i++) {
			if (probability > professions.get(i).getValue()) {
				professions.add(i, sd);
				added = true;
				break;
			}
		}

		if (!added && professions.size() < MAX_PROFESSIONS_COUNT) {
			professions.add(sd);
			return;
		}

		if (professions.size() > MAX_PROFESSIONS_COUNT)
			professions.removeLast();
	}

	/**
	 * Get up to three most likely professions. If {@link #check(StringDouble)}
	 * was called at least three times this guaranteed returns three
	 * professions.
	 * 
	 * Hackyhack by Calvin to get the top 5, then return last 3, cutting off first 2.
	 * 
	 * @return most likely professions, ordered by probability, with the most
	 *         likely one at index 0
	 */
	public String[] getProfessions() {
		String[] result = new String[professions.size()-2];
		for (int i = 2; i < professions.size(); i++)
			result[i-2] = professions.get(i).getString();

		return result;
	}
}
