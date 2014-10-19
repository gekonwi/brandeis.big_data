package code.profession;

import java.util.LinkedList;

import util.StringDouble;

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

		if (professions.isEmpty()) {
			professions.add(sd);
			return;
		}

		for (int i = 0; i < professions.size(); i++) {
			if (probability > professions.get(i).getValue()) {
				professions.add(i, sd);
				break;
			}
		}

		if (professions.size() > 3)
			professions.removeLast();
	}

	/**
	 * Get up to three most likely professions. If {@link #check(StringDouble)}
	 * was called at least three times this guaranteed returns three
	 * professions.
	 * 
	 * @return most likely professions, ordered by probability, with the most
	 *         likely one at index 0
	 */
	public String[] getProfessions() {
		String[] result = new String[professions.size()];
		for (int i = 0; i < professions.size(); i++)
			result[i] = professions.get(i).getString();

		return result;
	}

	/**
	 * Calls {@link #check(String, double)} with
	 * <code>profession = profProb.getString()</code> and
	 * <code>probability = profProb.getDouble()</code>.
	 * 
	 * @param profProb
	 */
	public void check(StringDouble profProb) {
		check(profProb.getString(), profProb.getValue());
	}
}
