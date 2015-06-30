package connection;



import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="driver")
public class DriverProperty {
	@XmlAttribute String name;			// This annotation assigns names to the elements in jdbc.xml -> order sensitive!
	@XmlAttribute String driverClass;
	@XmlAttribute String defaultPort;
	@XmlAttribute String urlPrefix;
	@XmlAttribute String dbNameSeparator;
	@XmlAttribute String quoteAliasOpen;
	@XmlAttribute String quoteAliasClose;
	@XmlAttribute String quoteEntityOpen;
	@XmlAttribute String quoteEntityClose;
	@XmlAttribute Boolean supportsCatalogs;	
	@XmlAttribute Boolean supportsSchemas;	
	@XmlAttribute Boolean supportsCreateTableAs;
	@XmlAttribute Boolean supportsWithData;
	@XmlAttribute String dateAddSyntax;
	@XmlAttribute String dateAddMonth;
	@XmlAttribute String dateDiffSyntax;
	@XmlAttribute String dateToNumber;
	@XmlAttribute String insertTimestampSyntax;
	@XmlAttribute String stdDevCommand;
	@XmlAttribute String charLengthCommand;
	@XmlAttribute String typeVarchar;
	@XmlAttribute String typeInteger;
	@XmlAttribute String typeDecimal;
	@XmlAttribute String typeTimestamp;
	@XmlAttribute String limitSyntax;
	@XmlAttribute String indexNameSyntax;
	@XmlAttribute String randomCommand;
	@XmlAttribute String testQuery;
}
