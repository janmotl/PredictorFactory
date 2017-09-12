package extraction;


import connection.Network;
import mother.PredictorMother;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import run.Setting;

import java.util.List;
import java.util.Set;

import static mother.PredictorMother.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class JournalTest {

	static Setting setting = new Setting("MariaDB", "mutagenesis_multiple");
	Journal journal;


	// Note: We do not need a real connection to the database - it can be mocked.
	@BeforeClass
	static public void init() {
		setting.predictorMax = 2;
		setting = Network.openConnection(setting);
	}

	@AfterClass
	static public void termination() {
		Network.closeConnection(setting);
	}

	@Before
	public void initEach() {
		journal = new Journal(setting, 2);
	}


	@Test
	public void unmarshall() {
		journal.addPredictor(setting, woeMutagenic());  //  0.5 0   woeMutagenic
		journal.addPredictor(setting, woeInd1());       //  0   0.8 woeInd1
		journal.addPredictor(setting, aggregateMax());  //  0.7 0.2 aggregateMax
		journal.addPredictor(setting, aggregateMin());

		Journal.marshall(journal);
		Journal unmarshalled = Journal.unmarshall();

		assertEquals(journal.getAllTopPredictors(), unmarshalled.getAllTopPredictors());
	}

	@Test
	public void respectLimit() {
		journal.addPredictor(setting, woeMutagenic());  //  0.5 0   woeMutagenic
		journal.addPredictor(setting, woeInd1());       //  0   0.8 woeInd1
		journal.addPredictor(setting, aggregateMax());  //  0.7 0.2 aggregateMax
		journal.addPredictor(setting, aggregateMin());  //  0.6 0.1 aggregateMin

		assertEquals(2, journal.getTopPredictors(mutagenic).size());
	}

	@Test
	public void properOrdering() {
		journal.addPredictor(setting, woeMutagenic());  //  0.5 0   woeMutagenic
		journal.addPredictor(setting, woeInd1());       //  0   0.8 woeInd1
		journal.addPredictor(setting, aggregateMax());  //  0.7 0.2 aggregateMax
		journal.addPredictor(setting, aggregateMin());  //  0.6 0.1 aggregateMin

		double mutagenic1 = journal.getTopPredictors(mutagenic).get(0).getChosenWeightedRelevance();
		assertEquals(0.7, mutagenic1, 0.00001);

		double mutagenic2 = journal.getTopPredictors(mutagenic).get(1).getChosenWeightedRelevance();
		assertEquals(0.6, mutagenic2, 0.00001);

		double ind1a = journal.getTopPredictors(ind1).get(0).getChosenWeightedRelevance();
		assertEquals(0.8, ind1a, 0.00001);

		double ind1b = journal.getTopPredictors(ind1).get(1).getChosenWeightedRelevance();
		assertEquals(0.2, ind1b, 0.00001);
	}

	@Test
	public void eviction() {
		Set<Predictor> p1 = journal.addPredictor(setting, woeMutagenic());   // 0.5  0
		Set<Predictor> p2 = journal.addPredictor(setting, woeInd1());        // 0    0.8
		Set<Predictor> p3 = journal.addPredictor(setting, aggregateMax());   // 0.7  0.2
		Set<Predictor> p4 = journal.addPredictor(setting, aggregateMin());   // 0.6  0.1
		Set<Predictor> p5 = journal.addPredictor(setting, aggregateAvg());   // 0.4  0.9

		assertTrue(p1.isEmpty());
		assertTrue(p2.isEmpty());
		assertTrue(p3.isEmpty());
		assertTrue(p4.size()==1);
		assertTrue(p5.isEmpty());
	}

	@Test
	public void evictionListContainsUniquePredictors() {
		Set<Predictor> p1 = journal.addPredictor(setting, aggregateStd());   // 0.5  0.05
		Set<Predictor> p2 = journal.addPredictor(setting, aggregateMin());   // 0.6  0.1
		Set<Predictor> p3 = journal.addPredictor(setting, aggregateMax());   // 0.7  0.2

		assertTrue(p1.isEmpty());
		assertTrue(p2.isEmpty());
		assertEquals(1, p3.size());
	}

	@Test
	public void containsOnClonedPredictors() {
		Predictor predictor = woeMutagenic();
		journal.addPredictor(setting, woeMutagenic());
		assertTrue(journal.containsPredictor(predictor)); // Check for the same object

		Predictor clone = new Predictor(predictor);
		assertTrue(journal.containsPredictor(clone)); // Check for the clone

		Predictor another = aggregateAvg();
		assertFalse(journal.containsPredictor(another)); // Check for a completely different object
	}

	@Test
	public void doesNotPermitValueDuplicates() {
		journal.addPredictor(setting, woeMutagenic());
		Predictor duplicate = woeMutagenic();
		duplicate.setId(543);
		journal.addPredictor(setting, duplicate);
		assertEquals(1, journal.getTopPredictors(mutagenic).size());
	}

	@Test
	public void selectsOnlyOneBestPredictorPerGroupAndTarget() {
		journal.addPredictor(setting, aggregateFrame1());
		assertEquals(1, journal.getTopPredictors(mutagenic).size());
		assertEquals(1, journal.getTopPredictors(ind1).size());

		journal.addPredictor(setting, aggregateFrame2());
		assertEquals(1, journal.getTopPredictors(mutagenic).size());
		assertEquals(1, journal.getTopPredictors(ind1).size());

		journal.addPredictor(setting, woeFrameMutagenic1());
		assertEquals(2, journal.getTopPredictors(mutagenic).size());
		assertEquals(1, journal.getTopPredictors(ind1).size());

		journal.addPredictor(setting, woeFrameMutagenic2());
		assertEquals(2, journal.getTopPredictors(mutagenic).size());
		assertEquals(1, journal.getTopPredictors(ind1).size());
	}

	@Test
	public void groupIdDuplicateWoE() {
		Predictor predictor = woeFrameMutagenic1();
		List<Predictor> obtained = journal.groupIdDuplicate(predictor);
		assertTrue(obtained.isEmpty());

		Predictor predictor2 = woeFrameMutagenic2();
		List<Predictor> obtained2 = journal.groupIdDuplicate(predictor2);
		assertEquals(1, obtained2.size());
	}

	@Test
	public void groupIdDuplicateAggregate() {
		Predictor predictor = aggregateFrame1();            // 0.2 0.1
		List<Predictor> obtained = journal.groupIdDuplicate(predictor);
		assertTrue(obtained.isEmpty());

		Predictor predictor2 = aggregateFrame2();
		List<Predictor> obtained2 = journal.groupIdDuplicate(predictor2);   // 0.1  0.3
		assertEquals(0, obtained2.size());

		Predictor predictor3 = aggregateFrame3();
		List<Predictor> obtained3 = journal.groupIdDuplicate(predictor3);   // 0.3  0.4
		assertEquals(2, obtained3.size());
	}

	@Test
	public void getAllTopPredictors() {
		assertEquals(0, journal.getAllTopPredictors().size());

		journal.addPredictor(setting, woeMutagenic());   // 0.5  0
		assertEquals(1, journal.getAllTopPredictors().size());

		journal.addPredictor(setting, woeInd1());        // 0    0.8
		assertEquals(2, journal.getAllTopPredictors().size());

		journal.addPredictor(setting, aggregateMax());   // 0.7  0.2
		assertEquals(3, journal.getAllTopPredictors().size());

		journal.addPredictor(setting, aggregateMin());   // 0.6  0.1
		assertEquals(3, journal.getAllTopPredictors().size());

		journal.addPredictor(setting, aggregateAvg());   // 0.4  0.9
		assertEquals(4, journal.getAllTopPredictors().size());
	}

	@Test
	public void getEvaluationCount() {
		assertEquals(0, journal.getEvaluationCount());

		journal.addPredictor(setting, woeMutagenic());   // 0.5  0
		assertEquals(1, journal.getEvaluationCount());

		journal.addPredictor(setting, woeInd1());        // 0    0.8
		assertEquals(2, journal.getEvaluationCount());

		journal.addPredictor(setting, aggregateMax());   // 0.7  0.2
		assertEquals(3, journal.getEvaluationCount());

		journal.addPredictor(setting, aggregateMin());   // 0.6  0.1
		assertEquals(4, journal.getEvaluationCount());

		journal.addPredictor(setting, aggregateAvg());   // 0.4  0.9
		assertEquals(5, journal.getEvaluationCount());
	}

	@Test
	public void zeroRelevance() {
		journal.addPredictor(setting, PredictorMother.zeroRelevance());   // 0  0
		assertEquals(0, journal.getAllTopPredictors().size());
	}

	@Test
	public void zeroConceptDrift() {
		journal.addPredictor(setting, PredictorMother.zeroConceptDrift());
		assertEquals(0, journal.getAllTopPredictors().size());
	}

	@Test
	public void fault() {
		journal.addPredictor(setting, faulty());
		assertEquals(0, journal.getAllTopPredictors().size());
	}
}
