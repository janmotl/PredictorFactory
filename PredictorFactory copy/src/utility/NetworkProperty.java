package utility;



import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="driver")
public class NetworkProperty {
	@XmlAttribute String name;
	@XmlAttribute String driver_class;
	@XmlAttribute String url;
	@XmlAttribute String username;
	@XmlAttribute String password;
	@XmlAttribute String quote_marks;
	@XmlAttribute String create_table_as_compatible;
	@XmlAttribute String schema_compatible;
	
	// Constructor
	public  NetworkProperty() {}
	
}
