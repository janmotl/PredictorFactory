package meta;

import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

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
	@XmlAttribute
	public String databaseName;   // Optional metadata just for users
	@XmlAttribute
	public String schemaName;     // Optional metadata just for users


	// More specific getter. Return all FC related to the table.
	// But if there is a self-referencing FC, include that FC just once.
	@NotNull public List<ForeignConstraint> getForeignConstraintList(String tableName) {

		// Initialisation
		List<ForeignConstraint> result = new ArrayList<>();

		// Select the appropriate foreign constrains
		for (ForeignConstraint foreignConstraint : this.foreignConstraint) {
			if (foreignConstraint.table.equals(tableName)) {
				result.add(foreignConstraint);
			} else if (foreignConstraint.fTable.equals(tableName)) {
				result.add(foreignConstraint);
			}
		}

		return result;
	}

	public void setForeignConstraint(List<ForeignConstraint> foreignConstraint) {
		this.foreignConstraint = foreignConstraint;
	}


	// Load property list from XML
	@NotNull public static ForeignConstraintList unmarshall(String fileName) {
		ForeignConstraintList list = new ForeignConstraintList();

		// Ignore absence of the file
		if (new File("config/" + fileName).isFile()) {
			try {
				JAXBContext jaxbContext = JAXBContext.newInstance(ForeignConstraintList.class);
				Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
				list = (ForeignConstraintList) jaxbUnmarshaller.unmarshal(new File("config/" + fileName));
			} catch (JAXBException ignored) {
				logger.warn("Attempt to parse 'config/" + fileName + "' failed.");
			}
		} else {
			list = new ForeignConstraintList();
			list.foreignConstraint = new ArrayList<>();
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
