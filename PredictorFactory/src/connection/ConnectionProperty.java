package connection;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="connection")
public class ConnectionProperty {
	@XmlAttribute String name;		// This annotation assigns names to the elements in connection.xml -> order sensitive!
	@XmlAttribute String driver;
	@XmlAttribute String host;
	@XmlAttribute String port;
	@XmlAttribute String database;
	@XmlAttribute String url;
	@XmlAttribute String username;
	@XmlAttribute String password;	
}
