package connection;

import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
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
			JAXBContext jaxbContext = JAXBContext.newInstance(DatabasePropertyList.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    list = (DatabasePropertyList) jaxbUnmarshaller.unmarshal( new File("config/database.xml") );
		} catch (JAXBException e) {
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
