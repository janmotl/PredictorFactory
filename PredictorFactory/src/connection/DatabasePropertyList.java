package connection;

import java.io.File;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement (name="databases")
@XmlAccessorType(XmlAccessType.FIELD)
public class DatabasePropertyList {
	private ArrayList<DatabaseProperty> database = new ArrayList<DatabaseProperty>();
	
	// Constructor
	public DatabasePropertyList(){}
	
	// Get property by name
	public DatabaseProperty getDatabaseProperties(String name) {
		for (DatabaseProperty properties : database) {
			if (properties.name.equals(name)) {
				return properties;
			}
		}
		
		System.out.println("There isn't a database setting for: " + name);
		return null;
	}
	
	// Load property list from XML
	public static DatabasePropertyList unmarshall(){
		DatabasePropertyList list = null;
	    
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(DatabasePropertyList.class);
		    Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
		    list = (DatabasePropertyList) jaxbUnmarshaller.unmarshal( new File("src/config/database.xml") );
		} catch (JAXBException e) {
			e.printStackTrace();
		}

	    return list;
	}
}
