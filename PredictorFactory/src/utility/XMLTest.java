package utility;

import org.testng.Assert;
import org.testng.annotations.Test;

public class XMLTest {

	@Test
	public static void validateXMLSchemaTest() {
    	Assert.assertEquals(XML.isXMLValid("src/resources/pattern.xsd", "src/pattern/direct_field.xml"), true);
    	Assert.assertEquals(XML.isXMLValid("src/resources/pattern.xsd", "src/pattern/aggregate.xml"), true);
    }

}
