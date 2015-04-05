package connection;



import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


@XmlType(name="database")
public class DatabaseProperty {
	@XmlAttribute String name;		// This annotation assigns names to the elements in database.xml -> order sensitive!
	@XmlAttribute String inputSchema;			
	@XmlAttribute String outputSchema;
	@XmlAttribute String targetTable;
	@XmlAttribute String targetId;
	@XmlAttribute String targetDate;
	@XmlAttribute String targetColumn;
	@XmlAttribute String blackListTable;
	@XmlAttribute String blackListColumn;
	@XmlAttribute String task;
	

	// Constructor
	public  DatabaseProperty() {}
	
}
