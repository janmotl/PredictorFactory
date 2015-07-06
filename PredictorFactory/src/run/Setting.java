/**
 * The struct with the global setting. 
 */
package run;

import java.sql.Connection;

//  Builder Pattern would be nice to make the variables final
public final class Setting {
	// Connection related
	public Connection connection; 				// The connection to the readServer
	public int indentifierLengthMax;			// The maximal length of a column/table
	public int columnMax;						// The maximal count of columns in a table
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
	public String targetId;			// The id column (like IdCustomer). Used only for base construction.
	public String targetDate;		// The date column. Used only for base construction.
	public String targetColumn;		// The target column. Used only for base construction.
	public String targetTable;		// The table with the target column. Used only for base construction.
	public String blackListTable;	// List of ignored tables. The syntax is like: ('tableName1', 'tableName2') 
	public String blackListColumn;	// List of ignored columns.The syntax is like: ('table1.column1', 'table2.column2')
	
	// Database logic
	public String inputSchema;
	public String outputSchema;
	public String task;				// Classification or regression?
	public boolean isTargetNominal = false;	// If the target is nominal, we have to escape the values
	
	// Names for entities created by Predictor Factory
	public String baseTable = "base";				// The name of the base table.
	public String baseId = "propagated_id";			// The name of the Id column. This name should be new & unique in input schema.	
	public String baseDate = "propagated_date";		// The date when the prediction is required. This name should be new & unique in input schema.	
	public String baseTarget = "propagated_target";	// The name of the target column. This name should be new & unique in input schema.
	public String baseFold = "propagated_fold";		// The name for fold in x-fold cross-validation.
	
	public String sampleTable = "mainSample";		// The name of the result table with predictors.
	public String journalTable = "journal"; 		// The name of predictors' journal table.
	
	int predictorStart = 100000;  					// Convenience for "natural sorting".
	
	public String propagatedPrefix = "propagated_";	// For single schema databases.
	public String predictorPrefix = "predictor";  	// Tables with predictors have uniform prefix.
	
	// Parameters
	public int propagationDepthMax = 10; 			// The maximal depth of base table propagation. Smaller value will result into faster propagation.
	public Integer lag = 240; 						// The amount of data history (in months) we allow the model to use when making the prediction.
	public Integer lead = 0;						// The period of time (in months) between the last data point the model can use to predict and the first data point the model actually predicts.
	public int sampleSize = 100;					// Downsample the base table to the given sample size per class (absent class is another class).
	//public boolean sample = true;				// If true, sample during propagation  
	public int valueCount = 20;						// Count of discrete values to consider in existencional quantifier.
	// missingValues (had to be implemented)
	
	// Constructor
 	public Setting() {
	}

	
}
