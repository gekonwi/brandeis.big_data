package code.profession;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TopProfessionsTest {

	@Test
	public void testThreeMostLikely() {
		TopProfessions prof = new TopProfessions();
		prof.check("soccer player", 0.5);
		prof.check("scientist", 0.1);
		prof.check("doctor", 0.3);
		prof.check("professor", 0.2);
		prof.check("social worker", 0.6);

		String[] topProfs = prof.getProfessions();

		assertEquals("Only the top 3 professions are stored", 3, topProfs.length);

		assertEquals("social worker", topProfs[0]);
		assertEquals("soccer player", topProfs[1]);
		assertEquals("doctor", topProfs[2]);
	}
}
