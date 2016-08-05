package connection;

import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;


@XmlRootElement (name="databases")
@XmlAccessorType(XmlAccessType.FIELD)
public class DatabasePropertyList {
	// Logging
	private static final Logger logger = Logger.getLogger(DatabasePropertyList.class.getName());
	
	// Private Fields
	private ArrayList<DatabaseProperty> database = new ArrayList<>();
	
	// Get property by name
	public DatabaseProperty getDatabaseProperties(String name) {
		for (DatabaseProperty properties : database) {
			if (properties.name.equals(name)) {
				return properties;
			}
		}
		
		logger.warn("There isn't a database setting for: " + name);
		return null;
	}
	
	// Set property by name
	public void setDatabaseProperties(DatabaseProperty property) {
		
		// Remove the old setting
		database.removeIf(i -> i.name.equals(property.name));
		
		// Add the new setting
		database.add(property);
	}
	
	// Load property list from XML
	public static DatabasePropertyList unmarshall(){
		DatabasePropertyList list = null;
	    
		try {
			JAXBContext context = JAXBContext.newInstance(DatabasePropertyList.class);
		    Unmarshaller unmarshaller = context.createUnmarshaller();

			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(DatabasePropertyList.class.getResource("/database.xsd"));

			// JAXB's default parser (Metro) does not take into account attributes' defaults defined in the xsd.
			// The implemented solution to the problem uses SAX parser instead.
			// See: http://stackoverflow.com/questions/5423414/does-jaxb-support-default-schema-values
			SAXParserFactory parser = SAXParserFactory.newInstance();
			parser.setSchema(schema);
			XMLReader xmlReader = parser.newSAXParser().getXMLReader();
			SAXSource source = new SAXSource(xmlReader, new InputSource(new FileInputStream("config/database.xml")));
			list = (DatabasePropertyList) unmarshaller.unmarshal(source);
		} catch (JAXBException|SAXException|FileNotFoundException|ParserConfigurationException e) {
			logger.warn("Attempt to parse 'config/database.xml' failed. Does the file exist?");
		}

		return list;
	}
	
	// Write into the XML
	public static void marshall(DatabasePropertyList databasePropertyList) {
	 	 
		try {
			File file = new File("config/database.xml");
			JAXBContext jaxbContext = JAXBContext.newInstance(DatabasePropertyList.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

			// Output pretty printed
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			
			// Write into the file
			jaxbMarshaller.marshal(databasePropertyList, file);
		} catch (JAXBException e) {
			logger.warn("Attempt to write 'config/database.xml' failed. Does Predictor Factory have the right to write?");
		}
	}
}
