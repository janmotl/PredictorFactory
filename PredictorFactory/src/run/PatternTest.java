package run;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;

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
		
		HashMap<String, String> parameter = new HashMap<String, String>();
		parameter.put("@aggregateFunction", "min,max");
		parameter.put("@aggregateFunction2", "min2,max2");
		pattern.setParameterMap(parameter);
		Assert.assertTrue(pattern.getParameterMap().containsKey("@aggregateFunction2"));
		Assert.assertFalse(pattern.getParameterMap().containsKey("@aggregateFunction3"));
	}

	@Test
	public void testMetaInformation() {
		Pattern pattern = new Pattern();
		pattern.setCode("CREATE TABLE @outputTable as SELECT @column1 FROM @inputTable1");

		ArrayList<String> arrayList = pattern.getParameters();

		Assert.assertEquals("@inputTable1", arrayList.get(1));
	}

}
