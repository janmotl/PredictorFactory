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
public class JDBCPropertyList {
	private ArrayList<JDBCProperty> driver = new ArrayList<JDBCProperty>();
	
	// Constructor
	public JDBCPropertyList(){}
	
	// Get property by name
	public JDBCProperty getJDBCProperties(String name) {
		for (JDBCProperty properties : driver) {
			if (properties.name.equals(name)) {
				return properties;
			}
		}
		
		System.out.println("There isn't a setting for: " + name);
		return null;
	}
	
	// Load property list from XML
	public static JDBCPropertyList unmarshall(){
		JDBCPropertyList list = null;
	    
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(JDBCPropertyList.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    list = (JDBCPropertyList) jaxbUnmarshaller.unmarshal( new File("src/connection/jdbc.xml") );
		} catch (JAXBException e) {
			e.printStackTrace();
		}

	    return list;
	}
}
