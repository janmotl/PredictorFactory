package featureExtraction;


import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;


public class PredictorTest {

	private Pattern pattern = new Pattern();
	private Predictor predictor;
		
	@Test
	public void clonePredictor() {
		pattern.name = "name";
		pattern.dialectCode = "select 3";
		pattern.author = "Thorough Tester";
		pattern.cardinality = "1";
		
		predictor = new Predictor(pattern);
		SortedMap<String, String> parameterList = new TreeMap<String, String>();
		parameterList.put("key2", "value4");
		predictor.setParameterMap(parameterList);
		
		Assert.assertEquals("select 3", predictor.getPatternCode());
		Assert.assertEquals("value4", predictor.getParameterMap().get("key2"));
	}
	

}
