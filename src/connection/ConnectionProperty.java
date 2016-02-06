package connection;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="connection")
public class ConnectionProperty {
	@XmlAttribute public String name;	// This annotation assigns names to the elements in connection.xml -> order sensitive!
	@XmlAttribute public String description;
	@XmlAttribute public String driver;
	@XmlAttribute public String host;
	@XmlAttribute public String port;
	@XmlAttribute public String database;
	@XmlAttribute public String url;	// Alternative for {host, port, database} - practical for Windows login at MSSQL,...
	@XmlAttribute public String username;
	@XmlAttribute public String password;
}
