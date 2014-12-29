package utility;

import java.time.LocalDate;
import java.util.SortedMap;
import java.util.TreeMap;

import org.testng.annotations.Test;
import org.testng.Assert;

import run.Pattern;

public class XMLTest {

	@Test
	public static void pattern2XMLTest() {
		String FILE_NAME = "src/pattern/pred.xml";
		Pattern pred = new Pattern();
		
		// Parameter: test setter & getter
		SortedMap<String, String> map = new TreeMap<String, String>();
		map.put("1", "a");
		map.put("2", "b");
		pred.setParameterMap(map);
		Assert.assertEquals(pred.getParameterMap().get("2"), "b");
		
		// Date: test setter & getter
		LocalDate now = LocalDate.now();
		pred.setDate(now);	
		Assert.assertEquals(pred.getDate(), now);
		
		// Save
		XML.pattern2xml(pred, FILE_NAME);
	}

	@Test
	public static void xml2patternTest() {
		String FILE_NAME = "src/pattern/pred.xml";
		Pattern patt = XML.xml2pattern(FILE_NAME);
		
		Assert.assertEquals(patt.getAuthor(), "Jan Motl");
		Assert.assertEquals(patt.getParameterMap().get("2"), "b");
	}
	
	@Test
	public static void validateXMLSchemaTest() {
    	Assert.assertEquals(XML.validateXMLSchema("src/resources/pattern.xsd", "src/pattern/direct_field.xml"), true);
    	Assert.assertEquals(XML.validateXMLSchema("src/resources/pattern.xsd", "src/pattern/aggregate.xml"), true);
    }

}
