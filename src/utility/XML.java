package utility;

import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.IOException;


public final class XML {
	// Logging
	private static final Logger logger = Logger.getLogger(XML.class.getName());

	// Validate the XML
	// The paths are relative to the class
	public static boolean isXMLValid(String xsdPath, String xmlPath) {
		try {
			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(XML.class.getResource(xsdPath));
			Validator validator = schema.newValidator();
			validator.validate(new StreamSource(xmlPath));
		} catch (@NotNull IOException | SAXException e) {
			logger.warn("Exception: " + e.getMessage());
			return false;
		}
		return true;
	}
}
