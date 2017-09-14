package meta;

import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "foreignConstraints")
public class ForeignConstraintList {
	// Logging
	private static final Logger logger = Logger.getLogger(ForeignConstraintList.class.getName());

	// Private Fields. Note that we are using List because it's marshallable by default.
	public List<ForeignConstraint> foreignConstraint;
	@XmlAttribute public String databaseName;   // Optional metadata just for users
	@XmlAttribute public String schemaName;     // Optional metadata just for users


	public void setForeignConstraint(List<ForeignConstraint> foreignConstraint) {
		this.foreignConstraint = foreignConstraint;
	}


	// Load property list from XML
	public static ForeignConstraintList unmarshall(String fileName) {
		ForeignConstraintList list = new ForeignConstraintList();

		// Ignore absence of the file
		if (new File("config/" + fileName).isFile()) {
			try {
				JAXBContext jaxbContext = JAXBContext.newInstance(ForeignConstraintList.class);
				Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
				list = (ForeignConstraintList) jaxbUnmarshaller.unmarshal(new File("config/" + fileName));
                logger.info("In total " + list.foreignConstraint.size() + " foreign key constraints were loaded from the XML file.");
			} catch (JAXBException ignored) {
				logger.warn("Attempt to parse 'config/" + fileName + "' failed.");
			}
		} else {
			list = new ForeignConstraintList();
			list.foreignConstraint = new ArrayList<>();
            logger.debug("File 'config/" + fileName + "' does not exist. Skipping the XML import.");
		}

		return list;
	}

	// Write into the XML
	public static void marshall(ForeignConstraintList foreignConstraint, String fileName) {

		try {
			File file = new File("config/" + fileName);
			JAXBContext jaxbContext = JAXBContext.newInstance(ForeignConstraintList.class);
			Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

			// Output pretty printed
			jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

			// Write into the file
			jaxbMarshaller.marshal(foreignConstraint, file);
		} catch (JAXBException ignored) {
			logger.warn("Attempt to write 'config/" + fileName + "' failed. Does Predictor Factory have the right to write?");
		}
	}
}
