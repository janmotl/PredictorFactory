package run;

import java.time.LocalDate;
import java.util.SortedMap;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PatternTest {

	@Test
	public void testSettersAndGetters() {
		// Author
		Pattern pattern = new Pattern();
		pattern.setAuthor("Test Run");
		Assert.assertEquals("Test Run", pattern.getAuthor());
		
		// Date
		LocalDate date = LocalDate.now();
		pattern.setDate(date);
		Assert.assertEquals(date, pattern.getDate());
		
		// Code
		pattern.setCode("SELECT *");
		Assert.assertEquals("SELECT *", pattern.getCode());
		
		// Cardinality
		pattern.setCardinality("n");
		Assert.assertEquals("n", pattern.getCardinality());
		
		// Parameter
		System.out.println(pattern.getParameterMap().get(0));
		
		SortedMap<String, String> parameter = new TreeMap<String, String>();
		parameter.put("@aggregateFunction", "min,max");
		parameter.put("@aggregateFunction2", "min2,max2");
		pattern.setParameterMap(parameter);
		Assert.assertTrue(pattern.getParameterMap().containsKey("@aggregateFunction2"));
		Assert.assertFalse(pattern.getParameterMap().containsKey("@aggregateFunction3"));
	}

}
