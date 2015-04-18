package connection;

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
public class DriverPropertyList {
	private ArrayList<DriverProperty> driver = new ArrayList<DriverProperty>();
	
	// Constructor
	public DriverPropertyList(){}
	
	// Get property by name
	public DriverProperty getDriverProperties(String name) {
		for (DriverProperty properties : driver) {
			if (properties.name.equals(name)) {
				return properties;
			}
		}
		
		System.out.println("There isn't a driver setting for: " + name);
		return null;
	}
	
	// Load property list from XML
	public static DriverPropertyList unmarshall(){
		DriverPropertyList list = null;
	    
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(DriverPropertyList.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    list = (DriverPropertyList) jaxbUnmarshaller.unmarshal( new File("src/config/driver.xml") );
		} catch (JAXBException e) {
			e.printStackTrace();
		}

	    return list;
	}
}
