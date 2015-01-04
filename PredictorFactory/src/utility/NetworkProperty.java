package utility;



import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="driver")
public class NetworkProperty {
	@XmlAttribute String name;			// This annotation assigns names to the elements in db.xml -> order sensitive!
	@XmlAttribute String driverClass;
	@XmlAttribute String url;
	@XmlAttribute String username;
	@XmlAttribute String password;
	@XmlAttribute String quoteMarks;
	@XmlAttribute String createTableAsCompatible;
	@XmlAttribute String schemaCompatible;
	@XmlAttribute String dateAddSyntax;
	@XmlAttribute String stdDevCommand;
	@XmlAttribute String dateTimeCompatible;
	
	// Constructor
	public  NetworkProperty() {}
	
}
