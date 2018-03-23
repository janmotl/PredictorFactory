/**
 * The struct with the global setting.
 */
package run;

import com.zaxxer.hikari.HikariDataSource;
import connection.*;
import org.apache.log4j.Logger;
import utility.TextParser;

import java.sql.Timestamp;
import java.util.*;

import static com.google.common.base.MoreObjects.firstNonNull;

//  Builder Pattern would be nice to make the variables final.
//  Could make the variables static to be available from everywhere (and final, if possible).
//  We should break the setting into individual classes. Or at least build a hierarchy.
// NOTE: Do not copy the values. Use composition. It will violate Demeter law, but whatever.
public final class Setting {
	// Logging
	private static final Logger logger = Logger.getLogger(Setting.class.getName());

	// Connection related
	public HikariDataSource dataSource;         // Pool of connections
	public int identifierLengthMax;             // The maximal length of a column/table
	public Integer predictorMax;                // The maximal count of columns in a table
	public int predictorMaxTheory = Integer.MAX_VALUE;  // The maximal count of columns in a table
	public String username;
	public String password;
	public String host;
	public String port;
	public String urlPrefix;
	public String urlSuffix;
	public String dbNameSeparator;
	public String driverClass;
	public String testQuery;
	public String url;                          // Alternative to {host, port, database}. Allows specific commands.
	public String database;                     // The default database on the server
	public String databaseVendor;               // MySQL, MS SQL, PostgreSQL...
	public String quoteAliasOpen;               // column as "COLUMN"
	public String quoteAliasClose;              // column as "COLUMN"
	public String quoteEntityOpen;              // Characters for entity escaping
	public String quoteEntityClose;             // Characters for entity escaping
	public boolean supportsCreateTableAs;       // Use "create table as" syntax?
	public boolean supportsWithData;            // MonetDB syntax: "create table t2 as select * from t1 WITH DATA"
	public boolean supportsJoinUsing;           // Does the database support "join using" clause?
	public boolean supportsSelectExists;        // Does the database support "select exists(...)"?
	public boolean supportsCatalogs;            // Use "database.schema.table" or "schema.table"?
	public boolean supportsSchemas;             // Use "database.schema.table" or "database.table"?
	public String corrSyntax;                   // Syntax for correlation
	public String dateAddSyntax;                // Syntax for changing a date by a given amount
	public String dateAddMonth;                 // Leap year acknowledging syntax
	public String dateDiffSyntax;               // Syntax for difference of two dates
	public String dateToNumber;                 // Whenever we need to perform statistical operations on the time
	public String insertTimestampSyntax;        // Syntax for inserting a timestamp into journal
	public String limitSyntax;                  // Limit the count of returned row to a manageable limit
	public String indexNameSyntax;              // Use "idx_table_column" or "column"?
	public String stdDevSampCommand;            // MS SQL is using STDEV in place of STDDEV_SAMP
	public String stdDevPopCommand;             // MS SQL is using STDEVP in place of STDDEV_POP
	public String charLengthCommand;            // MS SQL is using LEN in place of CHAR_LENGTH
	public String randomCommand;                // Command to use for generating a decimal number in range 0..1
	public String typeVarchar;                  // Data types are used for journal table definition
	public String typeDecimal;                  // Data types are used for journal table definition
	public String typeInteger;                  // Data types are used for journal table definition
	public String typeTimestamp;                // Data types are used for journal table definition

	// Setup related
	// WHITELISTS AND BLACKLISTS SHOULD BE ARRAYLISTS (JUST LIKE TARGETID AND TARGET COLUMN)
	public List<String> targetIdList = new ArrayList<>();   // The id column (like IdCustomer).
	public String targetDate;       // The date column. Used only for base construction.
	public List<String> targetColumnList = new ArrayList<>();   // List of target columns. Use it only for base construction and reporting. Everything in between should use baseTargetList!
	public String targetTable;      // The table with the target column. Used only for base construction.
	public String whiteListSchema;   // List of used schemas.
	public String blackListSchema;   // List of ignored schemas. The syntax is like: ('schemaName1', 'schemaName2')
	public String whiteListTable;   // List of used tables.
	public String blackListTable;   // List of ignored tables. The syntax is like: ('schemaName1.tableName1', 'schemaName2.tableName2')
	public String whiteListColumn;  // List of used columns.
	public String blackListColumn;  // List of ignored columns.The syntax is like: ('schemaName1.table1.column1', 'schemaName2.table2.column2')

	// Database logic
	public List<String> inputSchemaList;
	public String outputSchema;

	// Names for entities created by Predictor Factory
	// Reasoning: We have to have to be able to rename targetColumn to baseTarget (a unique identifier not present
	// in the database) because a column named exactly like targetColumn may exist in tables other than in the
	// target table. Consequently, we could be confronted with the problem that we may have to join two tables, each
	// with columns named targetColumn, but different content. I see two ways out of it. Either each column
	// can have an alias, which is used only when necessary. Or we may proactively rename columns {targetColumn,
	// targetDate, targetId} to unique names.
	// Note: We do not validate that the generated names are not already present in the database. We rely on luck.
	// Note: SAS metadata functions expect schema nad table names in capitals, since it is using case-sensitive like operator.
	// If we create tables with small letters, SAS metadata functions will ignore these tables.
	public final String baseTable = "base";                 // The name of the base table.
	public final String baseIdPrefix = "propagated_id";           // The name of the Id column. This name should be new & unique in input schema to avoid name colisions.
	public final List<String> baseIdList = new ArrayList<>();// BaseId, but parsed.
	public final String baseDate = "propagated_date";       // The date when the prediction is required. This name should be new & unique in input schema.
	public final String baseTargetPrefix = "propagated_target";   // The name of the target column. This name should be new & unique in input schema.
	public final List<String> baseTargetList = new ArrayList<>();// BaseTarget, but parsed.
	public final String baseFold = "propagated_fold";       // The name for fold in x-fold cross-validation.
	public final String baseSampled = "base_sampled";       // The name of the sampled base table.
	public final String mainTablePrefix = "MAINSAMPLE";           // The name of the result table with predictors.
	public final String journalPredictor = "journal_predictor";   // The name of predictors' journal table.
	public final String journalTable = "journal_table";     // The name of propagation journal table.
	public final String journalTemporal = "journal_temporal";// The name of temporal constraints' journal table.
	public final String journalPattern = "journal_pattern"; // The name of the list of patterns.
	public final String journalRun = "journal_run";         // The name of the list of runs.
	public final String bkpPrefix = "bkp";                  // To be able to recover the old work.
	public final String propagatedPrefix = "propagated";    // For single schema databases.
	public final String predictorPrefix = "PREDICTOR";      // Tables with predictors have uniform prefix.
	public final int predictorStart = 100000;               // Convenience for "natural sorting".

	// Parameters
	public int propagationDepthMax = 10;            // The maximal depth of base table propagation. Smaller value will result into faster propagation.
	public String unit;                             // In which units to measure lag and lead
	public Integer lag;                             // The amount of data history we allow the model to use when making the prediction.
	public Integer lead;                            // The period of time between the last data point the model can use to predict and the first data point the model actually predicts.
	public int sampleCount;                         // Downsample the base table to the given sample size per class (absent class is another class).
	public String task;                             // Classification or regression?
	public String whiteListPattern;                 // Namely select the patterns to use. Should be a list.
	public String blackListPattern;                 // Ignore some of the patterns. Should be a list.
	public List<String> baseDateRange = new ArrayList<>(); // The range of baseDate (for time constraint estimation).
	//public boolean sample = true;                 // If true, sample during propagation
	public int valueCount = 20;                     // Count of discrete values to consider in feature functions.
	// missingValues (had to be implemented)
	public String targetSchema;                     // Target table can be either in the input schema or output schema.
	public int secondMax;                           // Timeout on predictor calculation in seconds.
	public boolean useIdAttributes;                 // Use id attributes in feature creation?
	public boolean useTwoStages;                    // Perform exploration+exploitation, or directly calculate all predictors?
	public boolean isExploitationPhase;             // If two-phase processing is used, are we currently exploiting?
	public boolean ignoreDatabaseForeignConstraints;// By default foreign key constraints are read from the database.
	public boolean skipBaseGeneration = false;      // This is for iterative bug fixing...
	public boolean skipPropagation = false;
	public boolean skipAggregation = false;

	// Computed variables in Setting.java
	public Timestamp pivotDate;                     // A central timestamp in targetDate. Used in concept drift.
	public SQL dialect;                             // SQL commands for the specific database vendor

	// UGLY PLACE TO STORE IT (move it to metaInput/metaOutput/meta?)
	public Map<String, LinkedHashMap<String, Integer>> targetUniqueValueMap; // Unique values in the target columns: columnName -> {uniqueValues -> count}

	// Constructors
	public Setting() {
	}

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
		url = connectionProperty.url;
		databaseVendor = connectionProperty.driver;

		// Load driver properties
		driverClass = driverProperty.driverClass;
		dbNameSeparator = driverProperty.dbNameSeparator;
		urlPrefix = driverProperty.urlPrefix;
		testQuery = driverProperty.testQuery;
		dateToNumber = driverProperty.dateToNumber;
		charLengthCommand = driverProperty.charLengthCommand;
		randomCommand = driverProperty.randomCommand;
		quoteEntityOpen = driverProperty.quoteEntityOpen;
		quoteEntityClose = driverProperty.quoteEntityClose;

		// Load database properties
		inputSchemaList = TextParser.string2list(databaseProperty.inputSchema); // Permits input data in multiple schemas. In theory the default value could be targetSchema once XML is modified.
		outputSchema = firstNonNull(databaseProperty.outputSchema, inputSchemaList.get(0));
		targetSchema = firstNonNull(databaseProperty.targetSchema, inputSchemaList.get(0));
		targetTable = databaseProperty.targetTable;
		targetIdList = TextParser.string2list(databaseProperty.targetId); // Permits composite id
		targetDate = databaseProperty.targetDate;
		targetColumnList = TextParser.string2list(databaseProperty.targetColumn); // Permits multiple targets at once
		predictorMax = databaseProperty.predictorMax;

		// Load optional (null-able) properties. Always set appropriate default values.
		urlSuffix = firstNonNull(driverProperty.urlSuffix, "");
		quoteAliasOpen = firstNonNull(driverProperty.quoteAliasOpen, "\"");
		quoteAliasClose = firstNonNull(driverProperty.quoteAliasClose, "\"");
		indexNameSyntax = firstNonNull(driverProperty.indexNameSyntax, "table_idx");
		supportsCatalogs = firstNonNull(driverProperty.supportsCatalogs, true);
		supportsSchemas = firstNonNull(driverProperty.supportsSchemas, true);
		supportsCreateTableAs = firstNonNull(driverProperty.supportsCreateTableAs, true);
		supportsWithData = firstNonNull(driverProperty.supportsWithData, false);
		supportsJoinUsing = firstNonNull(driverProperty.supportsJoinUsing, false);
		supportsSelectExists = firstNonNull(driverProperty.supportsSelectExists, false);
		insertTimestampSyntax = firstNonNull(driverProperty.insertTimestampSyntax, "'@timestamp'");
		stdDevSampCommand = firstNonNull(driverProperty.stdDevSampCommand, "stdDev_samp");
		stdDevPopCommand = firstNonNull(driverProperty.stdDevPopCommand, "stdDev_pop");
		limitSyntax = firstNonNull(driverProperty.limitSyntax, "limit");
		dateAddSyntax = firstNonNull(driverProperty.dateAddSyntax, "DATEADD(@datePart, @amount, @baseDate)");
		dateAddMonth = firstNonNull(driverProperty.dateAddSyntax, "DATEADD(month, @amount, @baseDate)");
		dateDiffSyntax = firstNonNull(driverProperty.dateDiffSyntax, "DATEDIFF(day, @dateTo, @dateFrom)");
		typeVarchar = firstNonNull(driverProperty.typeVarchar, "VARCHAR");
		typeInteger = firstNonNull(driverProperty.typeInteger, "INTEGER");
		typeDecimal = firstNonNull(driverProperty.typeDecimal, "DECIMAL");
		typeTimestamp = firstNonNull(driverProperty.typeTimestamp, "TIMESTAMP");

		// Correlation:
		//  https://www.red-gate.com/simple-talk/blogs/statistics-sql-pearsons-correlation/
		//  http://stattrek.com/statistics/correlation.aspx?Tutorial=AP
		// If we divide by zero (i.e. at least one of the vectors is constant), we return zero:
		//  https://stackoverflow.com/questions/861778/how-to-avoid-the-divide-by-zero-error-in-sql
		// To avoid "Arithmetic overflow error converting expression to data type int" at Azure we cast to double.
		corrSyntax = firstNonNull(driverProperty.corrSyntax, "coalesce((Avg(1.0 * @column1 * @column2) - Avg(1.0*@column1) * Avg(@column2)) / nullif((stdDev_pop(@column1) * stdDev_pop(@column2)), 0), 0)");

		unit = firstNonNull(databaseProperty.unit, "year");
		lag = firstNonNull(databaseProperty.lag, 100);
		lead = firstNonNull(databaseProperty.lead, 0);
		task = firstNonNull(databaseProperty.task, "classification");
		sampleCount = firstNonNull(databaseProperty.sampleCount, Integer.MAX_VALUE);
		blackListPattern = firstNonNull(databaseProperty.blackListPattern, "");
		whiteListPattern = firstNonNull(databaseProperty.whiteListPattern, "");
		whiteListSchema = firstNonNull(databaseProperty.whiteListSchema, "");
		blackListSchema = firstNonNull(databaseProperty.blackListSchema, "");
		whiteListTable = firstNonNull(databaseProperty.whiteListTable, "");
		blackListTable = firstNonNull(databaseProperty.blackListTable, "");
		whiteListColumn = firstNonNull(databaseProperty.whiteListColumn, "");
		blackListColumn = firstNonNull(databaseProperty.blackListColumn, "");
		useIdAttributes = databaseProperty.useIdAttributes; // The default is set in xsd
		useTwoStages = databaseProperty.useTwoStages; // The default is set in xsd -> However, it always writes the value into XML -> not nice
		isExploitationPhase = firstNonNull(databaseProperty.isExploitationPhase, false);
        ignoreDatabaseForeignConstraints = firstNonNull(databaseProperty.ignoreDatabaseForeignConstraints, false);
		secondMax = firstNonNull(databaseProperty.secondMax, 0); // If zero, no timeout is applied

		// Initialize baseIdList based on the count of columns in targetIdList
		// Note: The first value could be without any index to make it nicer. Eg.: {propagated_id, propagated_id2}
		for (int i = 0; i < targetIdList.size(); i++) {
			baseIdList.add(baseIdPrefix + (i + 1));    // Indexing from 1 since that is the convention in SQL
		}

		// Initialize baseTargetList based on the count of columns in targetList
		for (int i = 0; i < targetColumnList.size(); i++) {
			baseTargetList.add(baseTargetPrefix + (i + 1));    // Indexing from 1 since that is the convention in SQL
		}

		// Set dialect
		switch (databaseVendor) {
			case "Oracle":
				dialect = new SQLOracle();
				break;
			default:
				dialect = new SQL();
		}

		// Log the configuration
		logger.debug("Entry name in connection.xml: " + connectionPropertyName);
		logger.debug("Entry name in database.xml: " + databasePropertyName);
		prettyPrint();
	}

	// Pretty print the configuration.
	// Useful to have this information in the log if the client has problems.
	private void prettyPrint() {
		logger.debug("Lag: " + lag);
		logger.debug("Lead: " + lead);
		logger.debug("Sample count limit: " + sampleCount);
		logger.debug("Predictor count limit: " + predictorMax);
		logger.debug("Timeout limit: " + secondMax);
		logger.debug("Use two stages: " + useTwoStages);
		logger.debug("Use ids: " + useIdAttributes);
		logger.debug("Target schema: " + targetSchema);
		logger.debug("Target table: " + targetTable);
		logger.debug("Target columns: " + targetColumnList);
		logger.debug("Target ids: " + targetIdList);
		logger.debug("Target timestamp: " + targetDate);
		logger.debug("Input schemas: " + inputSchemaList);
		logger.debug("Output schema: " + outputSchema);
	}
}
