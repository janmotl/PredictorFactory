package utility;

import java.io.File;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement (name="drivers")
@XmlAccessorType(XmlAccessType.FIELD)
public class NetworkPropertyList {
	private ArrayList<NetworkProperty> driver = new ArrayList<NetworkProperty>();
	
	// Constructor
	public NetworkPropertyList(){}
	
	// Get property by name
	public NetworkProperty getJDBCProperties(String name) {
		for (NetworkProperty properties : driver) {
			if (properties.name.equals(name)) {
				return properties;
			}
		}
		
		System.out.println("There isn't a setting for: " + name);
		return null;
	}
	
	// Load property list from XML
	public static NetworkPropertyList unmarshall(){
		NetworkPropertyList driverList = null;
	    
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(NetworkPropertyList.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    driverList = (NetworkPropertyList) jaxbUnmarshaller.unmarshal( new File("src/resources/db.xml") );
		} catch (JAXBException e) {
			e.printStackTrace();
		}

	    return driverList;
	}
}
