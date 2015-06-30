package utility;


import org.junit.Assert; 
import org.junit.Test;

public class XMLTest {

	@Test
	public void validateXMLSchemaTest() {
    	Assert.assertTrue(XML.isXMLValid("/resources/pattern.xsd", "pattern/direct_field.xml"));
    	Assert.assertTrue(XML.isXMLValid("/resources/pattern.xsd", "pattern/aggregate.xml"));
    }

}
