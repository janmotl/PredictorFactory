package extraction;


import mother.PatternMother;
import mother.PredictorMother;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PredictorTest {

	private Setting setting = new Setting();
	private Pattern pattern;
	private Predictor predictor;
		
	@Before
	public void init() {
		pattern = PatternMother.aggregate();
	}
	
	@Test
	public void setParameterMap() {
		predictor = new Predictor(pattern);
		predictor.setParameter("key2", "value4");
		
		assertEquals("select 3", predictor.getPatternCode());
		assertEquals("value4", predictor.getParameterMap().get("key2"));
	}

	@Test
	public void comparatorTwoDoubles() {
		// Initialization
		List<Predictor> predictorList = new ArrayList<>();
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		predictor.setRelevance("t", 0.5);
		predictor.setChosenBaseTarget("t");
		predictorList.add(predictor);
		
		Predictor predictor2 = new Predictor(pattern);
		predictor2.setId(2);
		predictor2.setRelevance("t", 0.8);
		predictor2.setChosenBaseTarget("t");
		predictorList.add(predictor2);

		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.SingleRelevanceComparator);
		
		assertEquals(2, predictorList.get(0).getId());
		assertEquals(1, predictorList.get(1).getId());
	}

	@Test
	public void equalityOfClone() {
		Predictor predictor = PredictorMother.aggregateAvg();
		Predictor cloned = new Predictor(predictor);

		assertTrue(predictor.equals(cloned));
	}


	@Test
	public void comparatorByCandidateState() {
		// Initialization
		List<Predictor> predictorList = new ArrayList<>();

		Predictor p1 = PredictorMother.woeMutagenic();
		p1.setChosenBaseTarget("mutagenic");
		p1.setCandidateState(1);
		p1.setRelevance("mutagenic", 0.5);
		predictorList.add(p1);

		Predictor p3 = PredictorMother.aggregateMax();
		p3.setChosenBaseTarget("mutagenic");
		p3.setCandidateState(-1);
		p3.setRelevance("mutagenic", 0.7);
		predictorList.add(p3);

		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.SingleRelevanceComparator);

		assertEquals(1, predictorList.get(0).getId());
		assertEquals(3, predictorList.get(1).getId());
	}

	@Test
	public void comparatorByRelevance() {
		// Initialization
		List<Predictor> predictorList = new ArrayList<>();

		Predictor p1 = PredictorMother.woeMutagenic();
		p1.setChosenBaseTarget("mutagenic");
		p1.setRelevance("mutagenic", 0.5);
		predictorList.add(p1);

		Predictor p3 = PredictorMother.aggregateMax();
		p3.setChosenBaseTarget("mutagenic");
		p3.setRelevance("mutagenic", 0.7);
		predictorList.add(p3);

		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.SingleRelevanceComparator);

		assertEquals(3, predictorList.get(0).getId());
		assertEquals(1, predictorList.get(1).getId());
	}

	@Test
	public void comparatorByRuntime() {
		// Initialization
		List<Predictor> predictorList = new ArrayList<>();

		Predictor p1 = PredictorMother.woeMutagenic();
		p1.setChosenBaseTarget("mutagenic");
		p1.setRelevance("mutagenic", 0.7);
		p1.setTimestampBuilt(LocalDateTime.now().minusYears(1));
		p1.setTimestampDelivered(LocalDateTime.now());
		predictorList.add(p1);

		Predictor p3 = PredictorMother.aggregateMax();
		p3.setChosenBaseTarget("mutagenic");
		p3.setRelevance("mutagenic", 0.7);
		p3.setTimestampBuilt(LocalDateTime.now().minusYears(2));
		p3.setTimestampDelivered(LocalDateTime.now());
		predictorList.add(p3);

		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.SingleRelevanceComparator);

		assertEquals(1, predictorList.get(0).getId());
		assertEquals(3, predictorList.get(1).getId());
	}

	@Test
	public void comparatorById() {
		// Initialization
		List<Predictor> predictorList = new ArrayList<>();
		LocalDateTime now = LocalDateTime.now();    // To guaranty that the runtimes are equal

		Predictor p1 = PredictorMother.woeMutagenic();
		p1.setChosenBaseTarget("mutagenic");
		p1.setRelevance("mutagenic", 0.7);
		p1.setTimestampBuilt(now.minusYears(1));
		p1.setTimestampDelivered(now);
		predictorList.add(p1);

		Predictor p3 = PredictorMother.aggregateMax();
		p3.setChosenBaseTarget("mutagenic");
		p3.setRelevance("mutagenic", 0.7);
		p3.setTimestampBuilt(now.minusYears(1));
		p3.setTimestampDelivered(now);
		predictorList.add(p3);

		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.SingleRelevanceComparator);

		assertEquals(1, predictorList.get(0).getId());
		assertEquals(3, predictorList.get(1).getId());
	}

	@Test
	public void weightedRelevance() {
		Predictor predictor = PredictorMother.woeInd1();
		predictor.setChosenBaseTarget("ind1");
		assertEquals(0.8, predictor.getWeightedRelevance("ind1"), 0.0001);
	}


	@Test
	public void trimTo() throws UnsupportedEncodingException {
		int limit = 4;
		String actual = Predictor.trimTo("_čččččččččččččččč", limit);

		System.out.println(actual);
		System.out.println(actual.length());
		System.out.println(actual.getBytes("UTF-8").length);
		assertTrue(actual.length() <= limit);
		assertTrue(actual.getBytes("UTF-8").length <= limit);
	}

	@Test
	public void nameLength_unicode() throws UnsupportedEncodingException {
		// Initialization
		setting.identifierLengthMax = 64;
		predictor = PredictorMother.aggregateMax();
		predictor.getTable().originalName = "tabčččččččččččččččččččččččččččččččččččččččččččččč";
		predictor.setParameter("par1", "valřřřřřřřřřřřřřřřřřřřřřřřř");
		predictor.setParameter("par2", "valžžžžžžžžžžžžžžžžžžžžžžžž");

		// Run
		String actual = predictor.getNameOnce(setting);

		System.out.println(actual);
		System.out.println(actual.length());
		System.out.println(actual.getBytes("UTF8").length);
		assertTrue(actual.length() <= setting.identifierLengthMax);
		assertTrue(actual.getBytes("UTF8").length <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_parameter() {
		// Initialization
		setting.identifierLengthMax = 64;
		predictor = PredictorMother.aggregateMax();
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_2parameters() {
		// Initialization
		setting.identifierLengthMax = 64;
		predictor = PredictorMother.aggregateMax();
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_patternName() {
		// Initialization
		setting.identifierLengthMax = 64;
		predictor = PredictorMother.aggregateMax();
		predictor.getPattern().name = "pattern89_123456789_123456789_123456789_123456789_123456789";
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_64_table() {
		// Initialization
		setting.identifierLengthMax = 64;
		predictor = PredictorMother.aggregateMax();
		predictor.getTable().originalName = "tab456789_123456789_123456789_123456789_123456789_123456789";
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		assertTrue(actual.length() <= setting.identifierLengthMax);
	}

	@Test
	public void nameLength_30_2parameters() {
		// Initialization
		setting.identifierLengthMax = 30;
		predictor = PredictorMother.aggregateMax();
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_30_patternName() {
		// Initialization
		setting.identifierLengthMax = 30;
		predictor = PredictorMother.aggregateMax();
		predictor.getPattern().name = "pattern89_123456789_123456789_123456789_123456789_123456789";
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		assertTrue(actual.length() <= setting.identifierLengthMax);
	}
	
	@Test
	public void nameLength_30_table() {
		// Initialization
		setting.identifierLengthMax = 30;
		predictor = PredictorMother.aggregateMax();
		predictor.getTable().originalName = "pattern89_123456789_123456789_123456789_123456789_123456789";
		predictor.setParameter("par1", "val456789_123456789_123456789_123456789_123456789_123456789");
		predictor.setParameter("par2", "val456789_123456789_123456789_123456789_123456789_123456789");
		
		// Run
		String actual = predictor.getNameOnce(setting);
		
		System.out.println(actual);
		System.out.println(actual.length());
		assertTrue(actual.length() <= setting.identifierLengthMax);
	}
}
