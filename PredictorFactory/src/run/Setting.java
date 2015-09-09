/**
 * The struct with the global setting. 
 */
package run;

import com.google.common.base.MoreObjects;
import connection.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//  Builder Pattern would be nice to make the variables final
public final class Setting {

	// Connection related
	public Connection connection; 				// The connection to the readServer
	public int identifierLengthMax;				// The maximal length of a column/table
	public int columnMax;						// The maximal count of columns in a table
	public String username;
	public String password;
	public String host;
	public String port;
	public String urlPrefix;
	public String dbNameSeparator;
	public String driverClass;
	public String testQuery;
	public String database;						// The default database on the server
	public String databaseVendor;				// MySQL, MS SQL, PostgreSQL...
	public String quoteAliasOpen;				// column as "COLUMN"
	public String quoteAliasClose;				// column as "COLUMN"
	public String quoteEntityOpen;				// Characters for entity escaping
	public String quoteEntityClose;				// Characters for entity escaping
	public boolean supportsCreateTableAs;		// Use "create table as" syntax?
	public boolean supportsWithData;			// MonetDB syntax: "create table t2 as select * from t1 WITH DATA"
	public boolean supportsCatalogs;			// Use "database.schema.table" or "schema.table"?
	public boolean supportsSchemas;				// Use "database.schema.table" or "database.table"?
	public String corrSyntax;					// Syntax for correlation
	public String dateAddSyntax;				// Syntax for changing a date by a given amount
	public String dateAddMonth;					// Leap year acknowledging syntax
	public String dateDiffSyntax;				// Syntax for difference of two dates
	public String dateToNumber;					// Whenever we need to perform statistical operations on the time 
	public String insertTimestampSyntax;		// Syntax for inserting a timestamp into journal
	public String limitSyntax;					// Limit the count of returned row to a manageable limit
	public String indexNameSyntax;				// Use "idx_table_column" or "column"?
	public String stdDevCommand;				// MS SQL is using STDEV in place of STDDEV_SAMP
	public String charLengthCommand;			// MS SQL is using LEN in place of CHAR_LENGTH
	public String randomCommand;				// Command to use for generating a decimal number in range 0..1
	public String typeVarchar;					// Data types are used for journal table definition
	public String typeDecimal;					// Data types are used for journal table definition
	public String typeInteger;					// Data types are used for journal table definition
	public String typeTimestamp;				// Data types are used for journal table definition

	// Setup related
	// WHITELISTS AND BLACKLISTS SHOULD BE ARRAYLISTS
	// ALSO TARGETID AND TARGET COLUMN SHOULD BE LISTS
	public List<String> targetIdList = new ArrayList<>();	// The id column (like IdCustomer).
	public String targetDate;		// The date column. Used only for base construction.
	public String targetColumn;		// The target column. Used only for base construction.
	public String targetTable;		// The table with the target column. Used only for base construction.
	public String whiteListTable;	// List of used tables.
	public String blackListTable;	// List of ignored tables. The syntax is like: ('tableName1', 'tableName2')
	public String whiteListColumn;	// List of used columns.
	public String blackListColumn;	// List of ignored columns.The syntax is like: ('table1.column1', 'table2.column2')
	
	// Database logic
	public String inputSchema;
	public String outputSchema;
	
	// Names for entities created by Predictor Factory
	public final String baseTable = "base";					// The name of the base table.
	public final String baseId = "propagated_id";			// The name of the Id column. This name should be new & unique in input schema to avoid name colisions.
	public final List<String> baseIdList = new ArrayList<>();// BaseId, but parsed.
	public final String baseDate = "propagated_date";		// The date when the prediction is required. This name should be new & unique in input schema.
	public final String baseTarget = "propagated_target";	// The name of the target column. This name should be new & unique in input schema.
	public final String baseFold = "propagated_fold";		// The name for fold in x-fold cross-validation.
	public final String baseSampled = "base_sampled";		// The name of the sampled base table.
	public final String mainTable = "mainSample";			// The name of the result table with predictors.
	public final String journalTable = "journal"; 			// The name of predictors' journal table.
	public final String journalPropagationTable = "journal_table"; 		// The name of propagation journal table.
	public final String propagatedPrefix = "propagated_";	// For single schema databases.
	public final String predictorPrefix = "PREDICTOR";  	// Tables with predictors have uniform prefix.
	public final int predictorStart = 100000;  				// Convenience for "natural sorting".
	
	// Parameters
	public final int propagationDepthMax = 10; 		// The maximal depth of base table propagation. Smaller value will result into faster propagation.
	public String unit;								// In which units to measure lag and lead
	public Integer lag; 							// The amount of data history (in months) we allow the model to use when making the prediction.
	public Integer lead;							// The period of time (in months) between the last data point the model can use to predict and the first data point the model actually predicts.
	public int sampleCount;							// Downsample the base table to the given sample size per class (absent class is another class).
	public String task;								// Classification or regression?
	public String whiteListPattern;					// Namely select the patterns to use. Should be a list.
	public String blackListPattern;					// Ignore some of the patterns. Should be a list.
	public boolean isTargetNominal = false;			// If the target is nominal, we have to escape the values
	//public boolean sample = true;					// If true, sample during propagation  
	public final int valueCount = 20;				// Count of discrete values to consider in existential quantifier.
	// missingValues (had to be implemented)
	public String targetSchema;						// Target table can be either in the input schema or output schema.
	
	// Constructors
	public Setting() {}

	public Setting(String connectionPropertyName, String databasePropertyName) {
 	   // Load the configuration XMLs
		ConnectionPropertyList connectionPropertyList = ConnectionPropertyList.unmarshall();
		DriverPropertyList driverPropertyList = DriverPropertyList.unmarshall(); 
		DatabasePropertyList databasePropertyList = DatabasePropertyList.unmarshall();
        
        // Get the configuration for the specified database vendor		
        ConnectionProperty connectionProperty = connectionPropertyList.getConnectionProperties(connectionPropertyName);
        DriverProperty driverProperty = driverPropertyList.getDriverProperties(connectionProperty.driver);
        DatabaseProperty databaseProperty = databasePropertyList.getDatabaseProperties(databasePropertyName);

		// Load connection properties
		database = connectionProperty.database;
		username = connectionProperty.username;
		password = connectionProperty.password;
		host = connectionProperty.host;
		port = connectionProperty.port;
		databaseVendor = connectionProperty.driver;

		// Load driver properties
		driverClass = driverProperty.driverClass;
		dbNameSeparator = driverProperty.dbNameSeparator;
		urlPrefix = driverProperty.urlPrefix;
		testQuery = driverProperty.testQuery;
		dateToNumber = driverProperty.dateToNumber;
		charLengthCommand = driverProperty.charLengthCommand;
		randomCommand = driverProperty.randomCommand;

		// Load database properties
		inputSchema = databaseProperty.inputSchema;
		outputSchema = databaseProperty.outputSchema;
		targetIdList = Arrays.asList(databaseProperty.targetId.split(","));	// Permit composite id
		targetDate = databaseProperty.targetDate;
		targetColumn = databaseProperty.targetColumn;
		targetTable = databaseProperty.targetTable;

        // Load optional (null-able) properties. Always set appropriate default values.  
 	   	quoteAliasOpen = MoreObjects.firstNonNull(driverProperty.quoteAliasOpen, "\"");
		quoteAliasClose = MoreObjects.firstNonNull(driverProperty.quoteAliasClose, "\"");
		indexNameSyntax = MoreObjects.firstNonNull(driverProperty.indexNameSyntax, "table_idx");
		supportsCatalogs = MoreObjects.firstNonNull(driverProperty.supportsCatalogs, true); 
		supportsSchemas = MoreObjects.firstNonNull(driverProperty.supportsSchemas, true); 
		supportsCreateTableAs = MoreObjects.firstNonNull(driverProperty.supportsCreateTableAs, true); 
		supportsWithData = MoreObjects.firstNonNull(driverProperty.supportsWithData, false);
		insertTimestampSyntax = MoreObjects.firstNonNull(driverProperty.insertTimestampSyntax, "'@timestamp'");
		stdDevCommand = MoreObjects.firstNonNull(driverProperty.stdDevCommand, "stddev_samp");
		limitSyntax = MoreObjects.firstNonNull(driverProperty.limitSyntax, "limit");
		dateAddSyntax = MoreObjects.firstNonNull(driverProperty.dateAddSyntax, "DATEADD(@datePart, @amount, @baseDate)");
		dateAddMonth = MoreObjects.firstNonNull(driverProperty.dateAddSyntax, "DATEADD(month, @amount, @baseDate)");
		dateDiffSyntax = MoreObjects.firstNonNull(driverProperty.dateDiffSyntax, "DATEDIFF(day, @dateTo, @dateFrom)");
		typeVarchar = MoreObjects.firstNonNull(driverProperty.typeVarchar, "VARCHAR");
		typeInteger = MoreObjects.firstNonNull(driverProperty.typeInteger, "INTEGER");
		typeDecimal = MoreObjects.firstNonNull(driverProperty.typeDecimal, "DECIMAL");
		typeTimestamp = MoreObjects.firstNonNull(driverProperty.typeTimestamp, "TIMESTAMP");

		// Note: the correct correlation is: select ((Avg(column1 * column2) - Avg(column1) * Avg(column2)) / (stdDev_samp(column1) * stdDev_samp(column2))), ((sum(column1 * column2) - count(*) * Avg(column1) * Avg(column2)) / (stdDev_samp(column1) * stdDev_samp(column2) * (count(*)-1))), stdDev_samp(column1), stdDev_samp(column2) FROM `predictor_factory`.`PREDICTOR100004` WHERE column2 is not null and column1 is not null
		corrSyntax = MoreObjects.firstNonNull(driverProperty.corrSyntax, "((Sum(@column1 * @column2) - count(*) * Avg(@column1) * Avg(@column2)) / ((count(*) - 1) * (stdDev_samp(@column1) * stdDev_samp(@column2))))");
		
		unit = MoreObjects.firstNonNull(databaseProperty.unit, "day");
		lag = MoreObjects.firstNonNull(databaseProperty.lag, 60);
		lead = MoreObjects.firstNonNull(databaseProperty.lead, 30);
		task = MoreObjects.firstNonNull(databaseProperty.task, "classification");
		sampleCount = MoreObjects.firstNonNull(databaseProperty.sampleCount, Integer.MAX_VALUE);
		blackListPattern = MoreObjects.firstNonNull(databaseProperty.blackListPattern, "");
		whiteListPattern = MoreObjects.firstNonNull(databaseProperty.whiteListPattern, "");
		whiteListTable = MoreObjects.firstNonNull(databaseProperty.whiteListTable, "");
		blackListTable = MoreObjects.firstNonNull(databaseProperty.blackListTable, "");
		whiteListColumn = MoreObjects.firstNonNull(databaseProperty.whiteListColumn, "");
		blackListColumn = MoreObjects.firstNonNull(databaseProperty.blackListColumn, "");
		targetSchema = MoreObjects.firstNonNull(databaseProperty.targetSchema, inputSchema);

		// Initialize baseIdList based on the count of columns in targetIdList
		for (int i = 0; i < targetIdList.size(); i++) {
			baseIdList.add("propagated_id" + (i+1));	// Indexing from 1
		}
 	}
	
 	// Provide brief description. Useful for documentation.
	public String toString() {
		return "Setting configuration: [[Lag: " + lag + "], [Lead: " + lead + "], [Sample count: " + sampleCount + "]]";
	}
}
