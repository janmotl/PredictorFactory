package connection;



import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="driver")
public class JDBCProperty {
	@XmlAttribute String name;			// This annotation assigns names to the elements in jdbc.xml -> order sensitive!
	@XmlAttribute String driverClass;
	@XmlAttribute String defaultPort;
	@XmlAttribute String urlPrefix;
	@XmlAttribute String integratedSecurity;
	@XmlAttribute String dbNameSeparator;
	@XmlAttribute String quoteMarks;
	@XmlAttribute String createTableAsCompatible;
	@XmlAttribute String schemaCompatible;
	@XmlAttribute String dateAddSyntax;
	@XmlAttribute String dateAddMonth;
	@XmlAttribute String dateDiffSyntax;
	@XmlAttribute String insertTimestampSyntax;
	@XmlAttribute String stdDevCommand;
	@XmlAttribute String charLengthCommand;
	@XmlAttribute String typeVarchar;
	@XmlAttribute String typeInteger;
	@XmlAttribute String typeDecimal;
	@XmlAttribute String typeTimestamp;
	@XmlAttribute String withData;
	
	// Constructor
	public  JDBCProperty() {}
	
}
