package connection;


import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/* 
	This file is separate from connection.xml as the properties can be set after the connection to the database.
	The tags are case sensitive!
	If the database doesn't support schemas (like MySQL), do not use {inputDatabase, outputDatabase}
	but use {inputSchema, outputSchema}. It's schizophrenic. But that's how MySQL implemented information_schema:
	database names are in "TABLE_SCHEMA". And "TABLE_CATALOG" is filled with a constant: "def". Similarly jdbc
	supports getSchema for all databases but getCatalog only for some.  
*/

@XmlType(name="database")
public class DatabaseProperty {
	@XmlAttribute public String name;		// This annotation assigns names to the elements in database.xml -> order sensitive!
	@XmlAttribute public String description;
	@XmlAttribute public String inputSchema;			
	@XmlAttribute public String outputSchema;
	@XmlAttribute public String targetSchema;
	@XmlAttribute public String targetTable;
	@XmlAttribute public String targetId;
	@XmlAttribute public String targetDate;
	@XmlAttribute public String targetColumn;
	@XmlAttribute public String task;
	@XmlAttribute public String whiteListPattern;
	@XmlAttribute public String blackListPattern;
	@XmlAttribute public String whiteListTable;
	@XmlAttribute public String blackListTable;
	@XmlAttribute public String whiteListColumn;
	@XmlAttribute public String blackListColumn;
	@XmlAttribute public String unit;	// The items {unit, lag, lead, sampleCount and task} should be somewhere else
	@XmlAttribute public Integer lag;
	@XmlAttribute public Integer lead;
	@XmlAttribute public Integer sampleCount;
	@XmlAttribute public Integer predictorMax;
	@XmlAttribute public Boolean useIdAttributes;
}
