package connection;

import java.io.File;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement (name="connections")
@XmlAccessorType(XmlAccessType.FIELD)
public class ConnectionPropertyList {
	private ArrayList<ConnectionProperty> connection = new ArrayList<ConnectionProperty>();
	
	// Constructor
	public ConnectionPropertyList(){}
	
	// Get property by name
	public ConnectionProperty getConnectionProperties(String name) {
		for (ConnectionProperty properties : connection) {
			if (properties.name.equals(name)) {
				return properties;
			}
		}
		
		System.out.println("There isn't a setting for: " + name);
		return null;
	}
	
	// Load property list from XML
	public static ConnectionPropertyList unmarshall(){
		ConnectionPropertyList list = null;
	    
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ConnectionPropertyList.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    list = (ConnectionPropertyList) jaxbUnmarshaller.unmarshal( new File("src/config/connection.xml") );
		} catch (JAXBException e) {
			e.printStackTrace();
		}

	    return list;
	}
}
