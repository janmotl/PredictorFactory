package connection;



import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="driver")
public class DriverProperty {
    @XmlAttribute public String name;           // This annotation assigns names to the elements in jdbc.xml -> order sensitive!
    @XmlAttribute public String driverClass;
    @XmlAttribute public String defaultPort;
    @XmlAttribute public String urlPrefix;
    @XmlAttribute public String urlSuffix;
    @XmlAttribute public String dbNameSeparator;
    @XmlAttribute public String quoteAliasOpen;
    @XmlAttribute public String quoteAliasClose;
    @XmlAttribute public String quoteEntityOpen;
    @XmlAttribute public String quoteEntityClose;
    @XmlAttribute public Boolean supportsCatalogs;
    @XmlAttribute public Boolean supportsSchemas;
    @XmlAttribute public Boolean supportsCreateTableAs;
    @XmlAttribute public Boolean supportsWithData;
    @XmlAttribute public Boolean supportsJoinUsing;
    @XmlAttribute public Boolean supportsSelectExists;
    @XmlAttribute public String corrSyntax;
    @XmlAttribute public String dateAddSyntax;
    @XmlAttribute public String dateAddMonth;
    @XmlAttribute public String dateDiffSyntax;
    @XmlAttribute public String dateToNumber;
    @XmlAttribute public String insertTimestampSyntax;
    @XmlAttribute public String stdDevCommand;
    @XmlAttribute public String charLengthCommand;
    @XmlAttribute public String typeVarchar;
    @XmlAttribute public String typeInteger;
    @XmlAttribute public String typeDecimal;
    @XmlAttribute public String typeTimestamp;
    @XmlAttribute public String limitSyntax;
    @XmlAttribute public String indexNameSyntax;
    @XmlAttribute public String randomCommand;
    @XmlAttribute public String testQuery;
}
