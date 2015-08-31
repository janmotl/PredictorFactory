package featureExtraction;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;


public class PredictorTest {

	private Pattern pattern = new Pattern();
	private Predictor predictor;
		
	@Before
	public void init() {
		pattern.name = "name";
		pattern.dialectCode = "select 3";
		pattern.author = "Thorough Tester";
		pattern.cardinality = "1";		
	}
	
	@Test
	public void setParameterMap() {
		predictor = new Predictor(pattern);
		SortedMap<String, String> parameterList = new TreeMap<String, String>();
		parameterList.put("key2", "value4");
		predictor.setParameterMap(parameterList);
		
		Assert.assertEquals("select 3", predictor.getPatternCode());
		Assert.assertEquals("value4", predictor.getParameterMap().get("key2"));
	}
	

	@Test
	public void comparatorTwoDoubles() {
		// Initialization
		List<Predictor> predictorList = new ArrayList<Predictor>();
		
		predictor = new Predictor(pattern);
		predictor.setId(1);
		predictor.setRelevance("t", 0.5);
		predictorList.add(predictor);
		
		Predictor predictor2 = new Predictor(pattern);
		predictor2.setId(2);
		predictor2.setRelevance("t", 0.8);
		predictorList.add(predictor2);
		
		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.RelevanceComparator.reversed());
		
		Assert.assertEquals(2, predictorList.get(0).getId());
		Assert.assertEquals(1, predictorList.get(1).getId());
	}
	
//	@Test
//	public void comparatorOneDouble() {
//		// Initialization
//		List<Predictor> predictorList = new ArrayList<Predictor>();
//
//		predictor = new Predictor(pattern);
//		predictor.setId(1);
//		predictorList.add(predictor);
//
//		Predictor predictor2 = new Predictor(pattern);
//		predictor2.setId(2);
//		predictor2.setRelevance("t", 0.8);
//		predictorList.add(predictor2);
//
//		// Sort the relevances in descending order
//		Collections.sort(predictorList, Predictor.RelevanceComparator.reversed());
//
//		//Assert.assertEquals(2, predictorList.get(0).getId());
//		//Assert.assertEquals(1, predictorList.get(1).getId());
//	}
}
