package extraction;


import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class RelevanceTest {

	@Test
	public void relevance() {
		Relevance relevance = new Relevance();
		assertEquals(0.0, relevance.getRelevance("baseTarget1"), 0.0001);

		relevance.setRelevance("baseTarget1", 32.4);
		assertEquals(32.4, relevance.getRelevance("baseTarget1"), 0.0001);

		relevance.setRelevance("baseTarget1", 2.4);
		assertEquals(2.4, relevance.getRelevance("baseTarget1"), 0.0001);

		relevance.setRelevance("baseTarget2", 56.0);
		assertEquals(2.4, relevance.getRelevance("baseTarget1"), 0.0001);
		assertEquals(56.0, relevance.getRelevance("baseTarget2"), 0.0001);
	}

}
