package utility;

import java.io.File;
import java.io.IOException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.xml.sax.SAXException;

import run.Pattern;

public final class XML {

	// Read an XML and return Pattern object
	public static Pattern xml2pattern(String fileName) {
		try {
			JAXBContext context = JAXBContext.newInstance(Pattern.class);
			Unmarshaller unMarshaller = context.createUnmarshaller();
			Pattern pattern = (Pattern) unMarshaller.unmarshal(new File(fileName));
			return pattern;
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return null;
	}

	// Write Pattern object into XML
	public static void pattern2xml(Pattern pred, String fileName) {
		try {
			JAXBContext context = JAXBContext.newInstance(Pattern.class);
			Marshaller m = context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

			// Write to File
			m.marshal(pred, new File(fileName));
		} catch (JAXBException e) {
			e.printStackTrace();
		}
	}

	// Validate the XML
	public static boolean validateXMLSchema(String xsdPath, String xmlPath) {
		try {
			SchemaFactory factory = SchemaFactory
					.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(new File(xsdPath));
			Validator validator = schema.newValidator();
			validator.validate(new StreamSource(new File(xmlPath)));
		} catch (IOException | SAXException e) {
			System.out.println("Exception: " + e.getMessage());
			return false;
		}
		return true;
	}

}
