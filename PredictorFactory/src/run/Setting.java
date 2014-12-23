package run;

import java.sql.Connection;

//  Builder Pattern would be nice to make the variables final
public final class Setting {
	// Connection related
	public Connection connection; 				// The connection to the readServer
	public String dbType;						// MySQL, MS SQL, PostgreSQL...
	public String quoteMarks;					// Characters for entity escaping
	public boolean isCreateTableAsCompatible;	
	public boolean isSchemaCompatible;

	
	// Setup related	
	public String idColumn;		// The id column (like IdCustomer). Used only for base construction.
	public String idTable;		// The table with the id column. Used only for base construction.
	public String targetDate;	// The date column. Used only for base construction.
	public String targetColumn;	// The target column. Used only for base construction.
	public String targetTable;	// The table with the target column. Used only for base construction.
	public String blackList;	// List of ignored tables. The syntax is like: ('tableName1', 'tableName2') 
	
	// Database logistic
	public String inputSchema;
	public String inputDatabase_name;
	public String outputSchema;
	public String outputDatabase_name;
	
	// Variable names
	public String baseTable;		// The name of the base table.
	public String baseId;			// The name of the Id column. This name should be new & unique in input schema.	
	public String baseDate;			// The date when the prediction is required. This name should be new & unique in input schema.	
	public String baseTarget;		// The name of the target column. This name should be new & unique in input schema.	
	
	public String sampleTable;		// The name of the result table with predictors.
	public String journalTable; 	// The name of predictors' journal table.
	public String statementTable; 	// The name of statements' journal table.
	
	int predictor_start = 1000;  	// Convenience for "natural sorting" 
	
	public String propagatedPrefix;	// For single schema databases
	public String predictorPrefix;  // Tables with predictors have uniform prefix
	
	
	// Constructor
 	public Setting() {
	}

	
}
