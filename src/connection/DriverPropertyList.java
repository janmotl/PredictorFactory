package connection;

import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.util.ArrayList;


@XmlRootElement (name="drivers")
@XmlAccessorType(XmlAccessType.FIELD)
public class DriverPropertyList {
    // Logging
    private static final Logger logger = Logger.getLogger(DriverPropertyList.class.getName());

    // Private Fields
    private ArrayList<DriverProperty> driver = new ArrayList<>();

    // Get property by name
    public DriverProperty getDriverProperties(String name) {
        for (DriverProperty property : driver) {
            if (property.name.equals(name)) {
                return property;
            }
        }
    
        logger.warn("There isn't a driver setting for: " + name);
        return new DriverProperty();
    }

    // Load property list from XML
    public static DriverPropertyList unmarshall(){
        DriverPropertyList list = null;
       
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(DriverPropertyList.class);
            Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
            list = (DriverPropertyList) jaxbUnmarshaller.unmarshal( new File("config/driver.xml") );
        } catch (JAXBException e) {
            logger.warn("Attempt to parse 'config/driver.xml' failed. Does the file exist?");
        }

        return list;
    }
}
