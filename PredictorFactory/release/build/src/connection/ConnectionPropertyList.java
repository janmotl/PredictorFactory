package connection;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;


@XmlRootElement (name="connections")
@XmlAccessorType(XmlAccessType.FIELD)
public class ConnectionPropertyList {
	// Logging
	public static final Logger logger = Logger.getLogger(ConnectionPropertyList.class.getName());
	
	// Private Fields. Note that we are using List because it's marshable by default. 
	private List<ConnectionProperty> connection = new ArrayList<ConnectionProperty>();
	
	// Get property by name
	public ConnectionProperty getConnectionProperties(String name) {
		for (ConnectionProperty properties : connection) {
			if (name.equals(properties.name)) {
				return properties;
			}
		}
		
		logger.warn("There isn't a connection setting for: " + name);
		return null;
	}
	
	// Set property by name
	public void setConnectionProperties(ConnectionProperty property) {
		
		// Remove the old setting
		connection.removeIf(i -> i.name.equals(property.name));
		
		// Add the new setting
		connection.add(property);
	}
	
	// Load property list from XML
	public static ConnectionPropertyList unmarshall(){
		ConnectionPropertyList list = null;
	    
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ConnectionPropertyList.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    list = (ConnectionPropertyList) jaxbUnmarshaller.unmarshal(new File("config/connection.xml"));
		} catch (JAXBException e) {
			logger.warn("Attempt to parse 'config/connection.xml' failed. Does the file exist?");
		}

	    return list;
	}
	
	// Write into the XML
	public static void marshall(ConnectionPropertyList connectionPropertyList) {
		 	 
		try {
			File file = new File("config/connection.xml");
			JAXBContext jaxbContext = JAXBContext.newInstance(ConnectionPropertyList.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

			// Output pretty printed
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			
			// Write into the file
			jaxbMarshaller.marshal(connectionPropertyList, file);
		} catch (JAXBException e) {
			logger.warn("Attempt to write 'config/connection.xml' failed. Does Predictor Factory have the right to write?");
		}
	}
}