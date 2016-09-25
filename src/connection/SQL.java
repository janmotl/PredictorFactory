package connection;

import com.google.common.collect.Lists;
import featureExtraction.Predictor;
import metaInformation.Column;
import metaInformation.MetaOutput;
import metaInformation.StatisticalType;
import metaInformation.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.log4j.Logger;
import parser.ANTLR;
import run.Setting;
import utility.Meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;

public final class SQL {
	// Logging
	private static final Logger logger = Logger.getLogger(SQL.class.getName());

	// Subroutine 1: Add "Create table as" sequence into the pattern. Moved from private -> protected for unit testing.
	protected static String addCreateTableAs(Setting setting, String sql) {

		// MSSQL syntax?
		if (!setting.supportsCreateTableAs) {
			sql = Parser.addIntoClause(sql);
		} else {
			sql = "CREATE TABLE @outputTable AS " + sql;
		}

		// MonetDB syntax?
		if (setting.supportsWithData) {
			sql = sql + " WITH DATA";
		}

		return sql;
	}

	// Subroutine 1.1: Add "Create table as" sequence into the pattern. Moved from private -> protected for unit testing.
	private static String addCreateViewAs(Setting setting, String sql) {

		sql = "CREATE VIEW @outputTable AS " + sql;

		return sql;
	}
		
	// Subroutine 2.1: Expand tables (like: Table -> Schema.Table)
	private static String expandName(String sql){
		// While MySQL doesn't implement "true" schemas, it implements information_schema
		// and places it next to the database (instead of inside the database). 
		// Hence purely based on the hierarchical structure we call MySQL's database as schema.
		sql = sql.replace("@inputTable", "@inputSchema.@inputTable");
		sql = sql.replace("@outputTable", "@outputSchema.@outputTable");
		sql = sql.replace("@baseTable", "@outputSchema.@baseTable");
		sql = sql.replace("@baseSampled", "@outputSchema.@baseSampled");
		sql = sql.replace("@targetTable", "@targetSchema.@targetTable");
		sql = sql.replace("@propagatedTable", "@outputSchema.@propagatedTable");
	
		return sql;
	}
	
	// Subroutine 2.2: Expand tables based on list (like: Table -> Schema.Table)
	private static String expandNameList(String sql, List<String> list) {

		// While MySQL doesn't implement "true" schemas, it implements information_schema
		// and places it next to the database (instead of inside the database).
		// Hence purely based on the hierarchical structure we call MySQL's database as schema.
		for (String field : list) {
			sql = sql.replace(field, "@outputSchema." + field);
		}
	
		return sql;
	}
	
	// Subroutine 3.1: Replace & escape the entities present in setting
	private static String escapeEntity(Setting setting, String sql, String outputTable) {
			// Test parameters
		if (setting.targetIdList == null || setting.targetIdList.isEmpty()) {
			throw new IllegalArgumentException("Target ID list is required");
		}
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL code is required");
		}
		if (StringUtils.isBlank(outputTable)) {
			throw new IllegalArgumentException("Output table is required");
		}
		if (StringUtils.isBlank(setting.baseTable)) {
			throw new IllegalArgumentException("Base table is required");
		}
		if (StringUtils.isBlank(setting.baseSampled)) {
			throw new IllegalArgumentException("Base sampled is required");
		}
		if (StringUtils.isBlank(setting.targetColumn)) {
			throw new IllegalArgumentException("Target column is required");
		}
		if (StringUtils.isBlank(setting.targetTable)) {
			throw new IllegalArgumentException("Target table is required");
		}
		if (StringUtils.isBlank(setting.inputSchema)) {
			throw new IllegalArgumentException("InputSchema is required");
		}
		if (StringUtils.isBlank(setting.outputSchema)) {
			throw new IllegalArgumentException("OutputSchema is required");
		}
		if (StringUtils.isBlank(setting.targetSchema)) {
			throw new IllegalArgumentException("TargetSchema is required");
		}
		if (setting.baseIdList == null || setting.baseIdList.isEmpty()) {
			throw new IllegalArgumentException("Base id list is required");
		}
		if (StringUtils.isBlank(setting.baseDate)) {
			throw new IllegalArgumentException("Base date is required");
		}
		if (StringUtils.isBlank(setting.baseTarget)) {
			throw new IllegalArgumentException("Base target is required");
		}

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape each part of targetId individually
		String escapedTargetId = "";
		for (String id : setting.targetIdList) {
			escapedTargetId = escapedTargetId + ", " + QL + id + QR;
		}
		escapedTargetId = escapedTargetId.substring(2);	// Remove the first two symbols

		// Escape each part of baseId individually
		String escapedBaseId = "";
		for (String id : setting.baseIdList) {
			escapedBaseId = escapedBaseId + ", " + QL + id + QR;
		}
		escapedBaseId = escapedBaseId.substring(2);	// Remove the first two symbols

		// Escape the entities
		sql = sql.replace("@baseId", escapedBaseId);
		sql = sql.replace("@baseDate", QL + setting.baseDate + QR);
		sql = sql.replace("@baseTarget", QL + setting.baseTarget + QR);
		sql = sql.replace("@baseFold", QL + setting.baseFold + QR);
		sql = sql.replace("@baseTable", QL + setting.baseTable + QR);
		sql = sql.replace("@baseSampled", QL + setting.baseSampled + QR);
		sql = sql.replace("@targetId", escapedTargetId);
		sql = sql.replace("@targetDate", QL + setting.targetDate + QR);
		sql = sql.replace("@targetColumn", QL + setting.targetColumn + QR);
		sql = sql.replace("@targetTable", QL + setting.targetTable + QR);
		sql = sql.replace("@inputSchema", QL + setting.inputSchema + QR);
		sql = sql.replace("@outputSchema", QL + setting.outputSchema + QR);
		sql = sql.replace("@targetSchema", QL + setting.targetSchema + QR);
		sql = sql.replace("@outputTable", QL + outputTable + QR);

		return sql;
	}

	// Subroutine 3.2: Replace & escape the entities from predictor fields
	private static String escapeEntityPredictor(Setting setting, String sql, Predictor predictor) {
	    // Test parameters
	    if (StringUtils.isBlank(sql)) {
	      throw new IllegalArgumentException("Code is required");
	    }
	    if (predictor.columnMap==null) {
		  throw new IllegalArgumentException("ColumnMap can not be null, but it can be empty");
		}
	    if (StringUtils.isBlank(predictor.propagatedTable)) {
	      throw new IllegalArgumentException("PropagatedTable is required");
	    }

	    // Get escape characters
	    String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

	    // Escape the entities
	    sql = sql.replace("@propagatedTable", QL + predictor.propagatedTable + QR);
	    sql = sql.replace("@columnName", escapeAlias(setting, predictor.getName()));
	    
	    for (String columnName : predictor.columnMap.keySet()) {
	    	sql = sql.replace(columnName, QL + predictor.columnMap.get(columnName) + QR);
		}

	    return sql;
	  }
	
	// Subroutine 3.3: Replace & escape the entities from map
	// IT COULD CALL escapeEntity to avoid the necessity to call 2 different escapeEntity* 
	private static String escapeEntityMap(Setting setting, String sql, Map<String, String> fieldMap) {
		// Test parameters
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("Code is required");
		}
		
		// Get escape characters
	    String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;
		
		// Escape the entities
		for (Map.Entry<String, String> field : fieldMap.entrySet()) {
			sql = sql.replace(field.getKey(), QL + field.getValue() + QR);
		}

		return sql;
	}

	private static String escapeEntityTable(Setting setting, String sql, MetaOutput.OutputTable table) {
		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape primitive entities
		sql = sql.replace("@inputTable", QL + table.originalName + QR);
		sql = sql.replace("@propagatedTable", QL + table.propagationTable + QR);
		sql = sql.replace("@outputTable", QL + table.name + QR);
		sql = sql.replace("@dateColumn", QL + table.temporalConstraint + QR);

		return sql;
	}

	private static String escapeEntity(Setting setting, String entity) {
		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape primitive entities
		return QL + entity + QR;
	}


	// Subroutine: Get escaped alias. Oracle is using double quotes. MySQL single quotes...
	private static String escapeAlias(Setting setting, String alias) {
		return setting.quoteAliasOpen + alias + setting.quoteAliasClose;
	}
	
	// Subroutine: Is the predictor a string or a number? Just ask the database.
	// Note: The data type could be predicted from the pattern and pattern parameters. But the implemented 
	// method is foolproof, though slow.
	private static String getPredictorType(Setting setting, String tableName, String columnName) {

		Table table = new Table();
		table.columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, tableName);
		table.categorizeColumns(setting);

		// NOTE: WE DO NOT WANT STATISTICAL TYPE, WE WANT RAW TYPE!
		if (table.getColumn(columnName).isTemporal) {
			return "temporal";
		} else if (table.getColumn(columnName).isNumerical) {
			return "numerical";
		} 
			
		return "nominal";
	}
	
	
	// Drop command
	public static boolean dropTable(Setting setting, String outputTable) {
		// Test parameters
		if (StringUtils.isBlank(outputTable)) {
			throw new IllegalArgumentException("Output table is required");
		}

		String sql = "DROP TABLE @outputTable";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);
		
		return Network.executeUpdate(setting.dataSource, sql);
	}

	// Drop command
	public static boolean dropView(Setting setting, String outputTable) {
		// Test parameters
		if (StringUtils.isBlank(outputTable)) {
			throw new IllegalArgumentException("Output table is required");
		}

		String sql = "DROP VIEW @outputTable";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);

		return Network.executeUpdate(setting.dataSource, sql);
	}


	// Remove all Predictor Factory related tables
	// Note: The function is using names from the setting object. If the current setting doesn't match the setting
	// with witch the tables were generated, the tables are NOT going to get dropped! 
	// Note: Deleting whole schema is not going to work, if it contains tables (at least in PostgreSQL).
	// Note: We could delete all tables in the schema. But I am terrified of consequences, if someone with administrator
	// privileges entered wrong output schema (for example, if someone swaps by accident input and output schema).
	// Also, if someone has set up Predictor Factory that inputSchema=outputSchema, we would delete all the user's data.
	// Note: Use addBatch() for speeding up if possible. Take care of memory limits as described at:
	// http://viralpatel.net/blogs/batch-insert-in-java-jdbc/
	// Note: Maybe this is the right place for making backup of mainsample and journal.
	// Note: We have to delete the tables/views in the reverse order of their creation because of dependencies if views
	// are used.
	public static void tidyUp(Setting setting) {
		// Initialization
		Map<String, Table> tableMap = utility.Meta.collectTables(setting, setting.database, setting.outputSchema);
		SortedMap<String, String> dropMap = new TreeMap<>();
		
		// Select tables for dropping
		for (String table : tableMap.keySet()) {
			if (table.startsWith(setting.mainTable)) dropMap.put(1 + table, table);			// MainSample and it's temporary tables
			if (table.startsWith(setting.predictorPrefix)) dropMap.put(2 + table, table);	// Predictors
			if (table.startsWith(setting.propagatedPrefix)) dropMap.put(3 + table, table);	// Propagated tables
			if (table.equals(setting.baseSampled)) dropMap.put(4 + table, table);			// Sampled base table
			if (table.equals(setting.baseTable)) dropMap.put(5 + table, table);				// Base table
			if (table.equals(setting.journalPredictor)) dropMap.put(6 + table, table);		// Journal table
			if (table.equals(setting.journalPattern)) dropMap.put(7 + table, table);		// Journal patterns
			if (table.equals(setting.journalTable)) dropMap.put(8 + table, table);			// Journal propagated table
		}

		// Drop the tables
		for (String table : dropMap.values()) {
			dropTable(setting, table);
		}
	}
	
	// Create index on {baseId, baseDate}.
	// Returns true if the update was successful.
	// Note: The index can not be unique because we are creating it on propagated tables (like table of transactions...).
	// Design note: There are troubles with obligatory index names (PostgreSQL allows to omit the name but other
	// databases like MySQL, Oracle, SAS or MSSQL require the index name) :
	//	1) The created index name can be already taken (e.g.: a user may have backed up the table from a previous run).
	//	2) The created index name can be too long. We could have truncated the index names, but then we could end up with duplicates.
	//  3) Naming conventions differ database from database. E.g. in SAS:
	// 		For a simple index, the index name must be the same as the column name.
	//		Literature: http://support.sas.com/documentation/cdl/en/proc/61895/HTML/default/viewer.htm#a002473673.htm
	//	   But PostgreSQL requires a unique index name per schema -> collisions could then happen.
	public static boolean addIndex(Setting setting, String outputTable) {

		// With SAS skip the indexing to avoid the need to comply with their naming convention
		if ("SAS".equals(setting.databaseVendor)) {
			return true;
		}

		// Otherwise just set the index
		String columns = "(@baseId)";
		if (setting.targetDate != null) {
			columns = "(@baseId, @baseDate)";
		}

		// We should be sure that the index name is not too long (because Oracle has a limit of 30 char).
		// Hence, strip away known names: ix_mainSample_temp100 -> ix_temp100.
		String name = "ix_" + outputTable;
		if (outputTable.startsWith(setting.propagatedPrefix)) {
			name =  "ix" + outputTable.substring(setting.propagatedPrefix.length(), outputTable.length());
		} else if (outputTable.startsWith(setting.mainTable)) {
			name = "ix" + outputTable.substring(setting.mainTable.length(), outputTable.length());
		}

		String sql = "CREATE INDEX " + name + " ON @outputTable " + columns;

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);
		
		boolean isOK = Network.executeUpdate(setting.dataSource, sql);
		
		return isOK;
	}
	
	// Set primary key for a table in the output schema.
	// Design note: On Azure it is necessary to first set the column as not null. Hence, we first set not-null constraint.
	// Design note: There are generally just two ways how to create a table with a primary key constraint:
	//	1) create table with primary key first, and use SELECT into later
	//	2) create table as first, and use add primary key later
	// Of the two options, creating the primary key (and its associated index) after data is loaded will probably be faster.
	// Alternatives in MySQL, Snowflake,... include CREATE TABLE LIKE. But that does not permit modification
	// of the table content.
	public static boolean setPrimaryKey(Setting setting, String outputTable) {
		String columns = "(@baseId)";
		if (setting.targetDate != null) {
			columns = "(@baseId, @baseDate)";
		}

		// Unique constraint first, but not on Oracle because it would result into:
		// 		ORA-02261: such unique or primary key already exists in the table
		// error.
		// Azure also requires Not-Null constraint. But to set the constraint we have to repeat the data type...
		// Hence, just skip creation of unique constraint.
//		String sql = "ALTER TABLE @outputTable ADD UNIQUE " + columns;
//		sql = expandName(sql);
//		sql = escapeEntity(setting, sql, outputTable);
//		Network.executeUpdate(setting.dataSource, sql);

		// Primary key at the end
		String sql = "ALTER TABLE @outputTable ADD PRIMARY KEY " + columns;
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);
		boolean isOK = Network.executeUpdate(setting.dataSource, sql);

		return isOK;
	}

	// Get rowCount for a table in the output schema.
	// SHOULD BE IN META OR USE BOOLEAN useInputSchema
	// IS NOT USING SYSTEM ESCAPING
	public static int getRowCount(Setting setting, String schema, String table) {
		String entity = setting.quoteEntityOpen + schema + setting.quoteEntityClose;
		entity = entity + "." + setting.quoteEntityOpen + table + setting.quoteEntityClose;
		String sql = "SELECT count(*) FROM " + entity;

		// Dirt cheap approach how to get row count is unfortunately database specific. Hence universal count(*) 
		// is implemented. Some databases, like MySQL, returns the answer to count(*) query immediately. In other 
		// databases, like PostgreSQL, you may have to wait ~200ms.
		// Note also, that JDBC 'metadata' method may indeed be slower than plain count(*) as it has to collect
		// and return more information than just the rowCount. This seems to be the problem with Teradata. 	
		
		List<String> resultList = Network.executeQuery(setting.dataSource, sql);
		
		// If the table doesn't exist, the resultSet is empty. Return 0.
		if (resultList.isEmpty()) return 0;
		
		// Otherwise return the actual row count
		return (int)Double.parseDouble(resultList.get(0)); // SAS can return 682.0. SHOULD IMPLEMENT LIST<INTEGERS>.
	}

	// Returns the minimum and maximum date a column in a form of a number stored as string.
	// It may be ugly, but by converting the dates into a number we don't have to deal with timestamps, datetimes...
	// NOTE: It is intended for collection of time values. Maybe it would be better to create a table in the database?
	public static List<String> getDateRange(Setting setting, String table, String column) {

		// Sometimes databases return t/f sometimes 1/0...
		String sql = "SELECT dateToNumber(min(@column)) FROM @outputTable UNION ALL SELECT dateToNumber(max(@column)) FROM @outputTable";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);
		sql = ANTLR.parseSQL(setting, sql);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@outputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		List<String> resultList = Network.executeQuery(setting.dataSource, sql);
		return resultList;
	}


	// Get count of non-null records
	// Useful for QC of the predictors
	// SHOULD BE IN META OR USE BOOLEAN useInputSchema
	// IS NOT USING SYSTEM ESCAPING
	public static int getNotNullCount(Setting setting, String schema, String table, String column) {
		String entity = setting.quoteEntityOpen + schema + setting.quoteEntityClose;
		entity = entity + "." + setting.quoteEntityOpen + table + setting.quoteEntityClose;

		// By default count over a column ignores NULL values
		String sql = "SELECT count(@column) FROM " + entity;
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);
		
		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		sql = escapeEntityMap(setting, sql, fieldMap);
		
		List<String> resultList = Network.executeQuery(setting.dataSource, sql);
		
		// If the table doesn't exist, the resultSet is empty. Return 0.
		if (resultList.isEmpty()) return 0;
		
		return (int)Double.parseDouble(resultList.get(0)); // SAS can return 682.0
	}

	// Returns true if the column contains null.
	public static boolean containsNull(Setting setting, String table, String column) {

		if ("Oracle".equals(setting.databaseVendor)) {
			return containsNullOracle(setting, table, column); // A quick patch. Should use a factory.
		}

		String sql = "SELECT exists(SELECT 1 FROM @inputTable WHERE @column is null)";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@inputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		return Network.isTrue(setting.dataSource, sql);
	}

	private static boolean containsNullOracle(Setting setting, String table, String column) {
		String sql = "SELECT count(*) FROM @inputTable WHERE @column is null";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@inputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) > 0;
	}

	// Returns true if the date column contains a date from the future.
	// NOTE: May fail on a timestamp or different database dialect.
	public static boolean containsFutureDate(Setting setting, String table, String column) {

		// The good thing on "current_date" is that it is a standard. The bad thing is that it does not work in MSSQL.
		// Hence we use ODBC standard of "{fn NOW()}", which should be automatically replaced in the JDBC driver to the
		// database specific command. The ODBC command works in PostgreSQL.
		// Plus the comparison works with: date, timestamp and datetime.
		// Comparison with time fails.

		if ("Oracle".equals(setting.databaseVendor)) {
			return containsFutureDateOracle(setting, table, column);
		}

		String sql = "SELECT exists(SELECT 1 FROM @inputTable WHERE {fn NOW()} < @column)";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@inputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		sql = ANTLR.parseSQL(setting, sql);

		return Network.isTrue(setting.dataSource, sql);
	}

	private static boolean containsFutureDateOracle(Setting setting, String table, String column) {
		String sql = "SELECT count(*) FROM (SELECT 1 FROM @inputTable WHERE {fn NOW()} < @column)";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@inputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		sql = ANTLR.parseSQL(setting, sql);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) > 0;
	}

	// Get count of tuples fulfilling the time constraint.
	// This is an optimistic estimate based on max(targetDate) and min(targetDate).
	public static int countUsableDates(Setting setting, String table, String column) {

		// First the upper bound (lead)
		String timeConstraint = " dateToNumber(" + setting.dateAddSyntax + ") <= " + setting.baseDateRange.get(1);
		timeConstraint = timeConstraint.replaceAll("@amount",  setting.lead.toString());

		// Then the lower bound (lag)
		timeConstraint = timeConstraint + " AND dateToNumber(" + setting.dateAddSyntax + ") >= " + setting.baseDateRange.get(0);
		Integer leadLag = setting.lead + setting.lag;
		timeConstraint = timeConstraint.replaceAll("@amount", leadLag.toString());

		// We do not work with the @baseDate but @column (it must be correctly escaped, hence, it is still a macro variable)
		timeConstraint = timeConstraint.replaceAll("@baseDate", "@column");

		String sql = "SELECT count(*) FROM @inputTable WHERE " + timeConstraint;

		// Set correct units
		sql = sql.replaceAll("@datePart", setting.unit);

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);
		sql = ANTLR.parseSQL(setting, sql);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@inputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		List<String> resultList = Network.executeQuery(setting.dataSource, sql);
		return (int)Double.parseDouble(resultList.get(0)); // SAS can return 682.0
	}


	// Get the maximal cardinality of the table in respect to targetId. If the cardinality is 1:1,
	// we may want to remove the bottom time constrain in base propagation.
	// Note that we are working with the input tables -> alter commands are forbidden.
	// IS NOT USING SYSTEM ESCAPING
	public static boolean isIdUnique(Setting setting, MetaOutput.OutputTable table) {

		if ("Oracle".equals(setting.databaseVendor)) {
			return isIdUniqueOracle(setting, table);
		}

		String sql = "SELECT exists(" +
						 "SELECT count(*) " +
						 "FROM @inputTable " +
						 "GROUP BY @idCommaSeparated " +
						 "HAVING count(*)>1" +
					 ")";

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape ids
		String idCommaSeparated = "";
		for (String id : table.propagationForeignConstraint.fColumn) {
			idCommaSeparated += QL + id + QR + ",";
		}
		idCommaSeparated = idCommaSeparated.substring(0, idCommaSeparated.length() - 1);
		sql = sql.replace("@idCommaSeparated", idCommaSeparated);


		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityTable(setting, sql, table);
		
		boolean result = Network.isResultSetEmpty(setting.dataSource, sql);
		
		if (result) logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " doesn't contain duplicates #");
		else logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " CONTAINS duplicates #");
		
		return result;
	}

	private static boolean isIdUniqueOracle(Setting setting, MetaOutput.OutputTable table) {
		String sql = "SELECT count(*) FROM (" +
				"SELECT count(*) " +
				"FROM @inputTable " +
				"GROUP BY @idCommaSeparated " +
				"HAVING count(*)>1" +
				")";

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape ids
		String idCommaSeparated = "";
		for (String id : table.propagationForeignConstraint.fColumn) {
			idCommaSeparated += QL + id + QR + ",";
		}
		idCommaSeparated = idCommaSeparated.substring(0, idCommaSeparated.length() - 1);
		sql = sql.replace("@idCommaSeparated", idCommaSeparated);

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityTable(setting, sql, table);

		boolean result = Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) == 0;

		if (result) logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " doesn't contain duplicates #");
		else logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " CONTAINS duplicates #");

		return result;
	}

	// Check whether the columns {baseId, baseDate} are unique in the table in the inputSchema.
	public static boolean isTargetTupleUnique(Setting setting, String table) {
		// We could have used possibly faster: "ALTER TABLE @outputTable ADD UNIQUE (@baseId, @baseDate)".
		// But it would not work in Netezza as Netezza doesn't support constraint checking and referential integrity.

		if ("Oracle".equals(setting.databaseVendor)) {
			return isTargetTupleUniqueOracle(setting, table);
		}

		String sql = "SELECT exists(" +
						"SELECT @targetId " +
						"FROM @targetTable " +
						"GROUP BY @targetId" + (setting.targetDate==null ? " " : ", @targetDate ") +
						"HAVING count(*)>1" +
				      ")";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return !Network.isTrue(setting.dataSource, sql); // Return negation
	}

	private static boolean isTargetTupleUniqueOracle(Setting setting, String table) {

		String sql = "SELECT count(*)" +
				"FROM (" +
				"SELECT @targetId " +
				"FROM @targetTable " +
				"GROUP BY @targetId" + (setting.targetDate==null ? " " : ", @targetDate ") +
				"HAVING count(*)>1" +
				") t";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) == 0;
	}

	// Returns 1 if the baseId in the table in the outputSchema is unique.
	public static boolean isTargetIdUnique(Setting setting, String table) {

		if ("Oracle".equals(setting.databaseVendor)) {
			return isTargetIdUniqueOracle(setting, table);
		}

		String sql = "SELECT exists(" +
					 "SELECT @baseId FROM @outputTable GROUP BY @baseId HAVING count(*)>1)";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return !Network.isTrue(setting.dataSource, sql); // Return negation
	}

	private static boolean isTargetIdUniqueOracle(Setting setting, String table) {
		String sql = "SELECT count(*) FROM (" +
				"SELECT @baseId FROM @outputTable GROUP BY @baseId HAVING count(*)>1)";


		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) == 0;
	}


	// Return (unsorted) list of unique records.
	// This function is useful for example for dummy coding of nominal attributes.
	// NOTE: IT WOULD BE NICE IF THE COUNT OF RETURNED SAMPLES WAS SORTED AND LIMITED -> a subquery?
	// NOTE: It would be nice, if a vector of occurrences was also returned (to skip rare configurations).
	public static List<String> getUniqueRecords(Setting setting, String tableName, String columnName, boolean useInputSchema) {
		String table = "@outputTable";
		if (useInputSchema) {
			table = "@inputTable";
		}

		String sql = "SELECT DISTINCT @columnName " +
				"FROM " + table + " " +
				"WHERE @columnName is not null";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, tableName);
		HashMap<String, String> map = new HashMap<>();
		map.put("@columnName", columnName);
		map.put("@inputTable", tableName); // To cover the scenario that it's in the input schema 
		sql = escapeEntityMap(setting, sql, map);

		return Network.executeQuery(setting.dataSource, sql);
	}


	// Return top N unique records sorted by the frequency in the descending order.
	// The N is given by setting.valueCount (if necessary, could become a parameter).
	// This function is useful for example for dummy coding of nominal attributes.
	// NOTE: It would be nice, if a vector of frequencies was also returned (to skip rare values
	// in absolute measure, not only relative - handy when the attribute's cardinality is small).
	// NOTE: Maybe I should consider null values as a legit category.
	public static List<String> getTopUniqueRecords(Setting setting, String tableName, String columnName) {
		String table = "@inputTable";

		String sql = "SELECT @columnName " +
				"FROM " + table + " " +
				"WHERE @columnName is not null " +
				"GROUP BY @columnName " +
				"ORDER BY count(*) DESC";

		// A query without an aggregate function in order by clause because of following limitation of SAS:
		//	Summary functions are restricted to the SELECT and HAVING clauses only...
		sql = "SELECT @columnName, count(*) " +
				"FROM " + table + " " +
				"WHERE @columnName is not null " +
				"GROUP BY @columnName " +
				"ORDER BY 2 DESC";

		//sql = Parser.limitResultSet(setting, sql, setting.valueCount); // Because of SAS we use JDBC maxRows
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, tableName);
		HashMap<String, String> map = new HashMap<>();
		map.put("@columnName", columnName);
		map.put("@inputTable", tableName); // To cover the scenario that it's in the input schema
		sql = escapeEntityMap(setting, sql, map);

		return Network.executeQuery(setting.dataSource, sql, setting.valueCount);
	}

	// Could the two columns in the table describe a symmetric relation (like in borderLength(c1, c2))?
	// DEVELOPMENTAL AND LIKELY USELESS...
	public static boolean isSymmetric(Setting setting, HashMap<String, String> map) {
		String sql = "SELECT exists("
						+ "SELECT @lagColumn, @column FROM @inputTable"
						+ "EXCEPT "
						+ "SELECT @column, @lagColumn FROM @inputTable"
					+ ")";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityMap(setting, sql, map);

		return Network.isTrue(setting.dataSource, sql);
	}
	
	// Get R2. For discrete variables, following method is used:
	// http://stats.stackexchange.com/questions/119835/correlation-between-a-nominal-iv-and-a-continuous-dv-variable
	// R2 is normalized by the count of non-null samples of the predictor to penalize for sparse predictors.
 	public static double getR2(Setting setting, String table, String column) {
 		// Initialization
 		String sql;
 		String dataType = getPredictorType(setting, table, column);
 		
 		// Is the predictor categorical, numeric or time? 		
 		if ("nominal".equals(dataType)) {
 			sql = "SELECT count(*)*power(corr(t2.average, t1.@baseTarget), 2) " +
					"FROM @outputTable t1 " +
					"JOIN ( " +
						"SELECT @column " + 
							", cast(avg(@baseTarget) as decimal(38, 10)) as average " +	// NOT NICE, but solves frequent arithmetic errors on MSSQL.
						"FROM @outputTable " +
						"GROUP BY @column " +
					") t2 " +
					"ON t1.@column = t2.@column" +
					"where t1.@column is not null AND t1.@baseTarget is not null";
 		} else if ("numerical".equals(dataType)){
 			sql = "SELECT count(@column)*power(corr(@column, @baseTarget), 2) " +
					"FROM @outputTable " + /* We are working with the outputTable, hence schema & database are output. */
					"WHERE @column is not null AND @baseTarget is not null";
 		} else {
 			sql = "SELECT count(@column)*power(corr(dateToNumber(@column), @baseTarget), 2) " +
					"FROM @outputTable " +
					"WHERE @column is not null AND @baseTarget is not null";
 		}

		// Replace the generic corr command with the database specific version.
		// Also take care of dateToNumber
		sql = ANTLR.parseSQL(setting, sql);

		// Take care of stdDev (needed if the corr command is assembled from the basic commands)
		sql = sql.replaceAll("stdDev_samp", setting.stdDevCommand);
 		
		// Escape the entities
 		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);	/* outputTable, outputSchema, output.Database */
		HashMap<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@baseTarget", setting.baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);
		
		// Execute the SQL
		List<String> response = Network.executeQuery(setting.dataSource, sql);
		
		// Parse the response
		double correlation;

		try {
			correlation = Double.parseDouble(response.get(0)); 
		} catch (Exception e){
			logger.info("The response in correlation calculation is null (constant values, empty table...), return 0.");
			correlation = 0;
		}
		
		return correlation;
	}
	
 	// Get Chi2.
	// Note: For maintenance purpose, the query should be decomposed into subqueries
	// that are put together with String concatenation (necessary for databases without "with" clause support).
	// The calculation could be streamlined as described in:
	//	Study of feature selection algorithms for text-categorization.
	// SHOULD BE EXTENDED TO SUPPORT BOOLEANS
 	public static double getChi2(Setting setting, String table, String column) {
 		// Initialization
 		String sql;
 		String predictorType = getPredictorType(setting, table, column);
 		
 		// Technical: MySQL or PostgreSQL require correlation name (also known as alias) for derived tables. 
 		// But Oracle does not accept "as" keyword in front of the table alias. See:
 		// http://www.techonthenet.com/oracle/alias.php
 		// Hence use following syntax: (SELECT col1 from table t1) t2.
		// Linear regularization is appropriate assuming infinite samples. On finite samples it is possibly too
		// harsh on attributes with too many unique values. But I can live with that.
 		if ("nominal".equals(predictorType)) {
 			// Use categorical column directly
 			sql = "SELECT sum(chi2)/count(distinct(bin)) " // Linearly regularized against columns with high cardinality
				+ "FROM ( "
				+ "	SELECT (expected.expected-measured.count) * (expected.expected-measured.count) / expected.expected AS chi2"
				+ " , expected.bin AS bin"
				+ "	FROM ( "
				+ "		SELECT expected_bin.count*expected_target.prob AS expected "
				+ "			 , bin "
				+ "			 , target "
				+ "		FROM ( "
				+ "			SELECT @baseTarget AS target "
				+ "				 , count(*)/max(t2.nrow) AS prob "
				+ "			FROM @outputTable, ( "
				+ "				SELECT count(*) AS nrow "
				+ "				FROM @outputTable "
				+ "			) t2 "
				+ "			GROUP BY @baseTarget "
				+ "		) expected_target, ( "
				+ "			SELECT count(*) AS count "
				+ "				 , @column AS bin "
				+ "			FROM @outputTable "
				+ "			GROUP BY @column "
				+ "		) expected_bin "
				+ "	) expected "
				+ "	LEFT JOIN ( "
				+ "		SELECT @baseTarget AS target "
				+ "			 , count(*) AS count "
				+ "			 , @column AS bin "
				+ "		FROM @outputTable "
				+ "		GROUP BY @column, @baseTarget "
				+ "	) measured "
				+ "	ON expected.bin = measured.bin "
				+ "	AND expected.target = measured.target "
				+ ") chi2";
 		} else {
 			// Group numerical/time values into 10 bins.
 			// If desirable you can optimize the optimal amount of bins with Sturge's rule 
 			// but syntax for log is different in each database. 
 			sql = "SELECT sum(chi2)/10 " // To match regularization of nominal columns
				+ "FROM ( "
				+ "	SELECT (expected.expected-measured.count) * (expected.expected-measured.count) / expected.expected AS chi2 "
				+ "	FROM ( "
				+ "		SELECT expected_bin.count*expected_target.prob AS expected "
				+ "			 , bin "
				+ "			 , target "
				+ "		FROM ( "
				+ "			SELECT @baseTarget AS target "
				+ "				 , count(*)/max(t2.nrow) AS prob "
				+ "			FROM @outputTable, ( "
				+ "				SELECT count(*) AS nrow "
				+ "				FROM @outputTable "
				+ "			) t2 "
				+ "			GROUP BY @baseTarget "
				+ "		) expected_target, ( "
				+ "			SELECT count(*) AS count "
				+ "				 , floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)) AS bin " // Bin really into 10 bins.
				+ "			FROM @outputTable, ( "
				+ "					SELECT (max(@column)-min(@column)) / 10 AS bin_width "
				+ "						 , min(@column) AS min_value "
				+ "					FROM @outputTable "
				+ "				) t2 "
				+ "			GROUP BY floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)) "	// And avoid division by zero.
				+ "		) expected_bin "
				+ "	) expected "
				+ "	LEFT JOIN ( "
				+ "		SELECT @baseTarget AS target "
				+ "			 , count(*) AS count "
				+ "			 , floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)) AS bin "
				+ "		FROM @outputTable, ( "
				+ "				SELECT (max(@column)-min(@column)) / 10 AS bin_width "
				+ "					 , min(@column) AS min_value "
				+ "				FROM @outputTable "
				+ "			) t2 "
				+ "		GROUP BY floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)), @baseTarget "
				+ "	) measured "
				+ "	ON expected.bin = measured.bin "
				+ "	AND expected.target = measured.target "
				+ ") chi2";

 			// For time columns just cast time to number.
 			if ("temporal".equals(predictorType)) {
 				sql = sql.replace("@column", setting.dateToNumber);
 			}
 		}

		// For databases that suffer from overflow whenever calculating an average, cast to DECIMAL.
		// Note that SAS does not support anything like DECIMAL/NUMERIC... Hence, code bifurcation is necessary.
		if (!"SAS".equals(setting.databaseVendor)) {
			sql = sql.replace("count(*)", "cast(count(*) as DECIMAL)");
		}

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);	/* outputTable, outputSchema, output.Database */
		
		// Escape the entities
		HashMap<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@baseTarget", setting.baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);

		// Execute the SQL
		List<String> response = Network.executeQuery(setting.dataSource, sql);
		
		// Parse the response. 
		double chi2;

		try {
			chi2 = Double.parseDouble(response.get(0)); 
		} catch (Exception e){	// Cover's both, number format exception and null pointer exception.
			chi2 = 0; 
			logger.info("The result of Chi2 calculation on " + table + "." + column + " is null (empty table...). Returning 0.");
		}
		
		return chi2;
		
	}
	
 	// QC patterns based on produced predictors
 	private static List<String> qcPredictors(Setting setting) {
 		String sql = "SELECT pattern_name " +
					 "FROM @outputTable " +
					 "GROUP BY pattern_name " +
					 "having avg(is_ok + 0.0) = 0";	// We have to cast int to double before averaging
 		
 		sql = expandName(sql);
 		sql = escapeEntity(setting, sql, setting.journalPredictor);
 		List<String> badPatterns = Network.executeQuery(setting.dataSource, sql);
 		
 		return badPatterns;
 	}
 	
 	// Log Predictor Factory total run time.
 	// ADD: column with count of produced predictors & whether PF finished successfully.
 	// NOTE: COPYING OF TABLES IS UGLY. GIVE IT DIRECTLY THE SPECIFIC NAMES AND MAKE GENERIC TABLES JUST AS A VIEW?
 	// NOTE: NECESSARY TO ADD COMMAND createTableAs BECAUSE OF MSSQL.
 	public static void logRunTime(Setting setting, long elapsedTime){
 		// Drop the previous version
 		dropTable(setting, "ms_" + setting.inputSchema);
 		
 		// Copy the current mainSample
 		String sql = "create table @outputSchema.@ms as SELECT * FROM @outputTable";
 		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.mainTable);
		HashMap<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@ms", "ms_" + setting.inputSchema);
		sql = escapeEntityMap(setting, sql, fieldMap);
		Network.executeUpdate(setting.dataSource, sql);
 		
		// Drop the previous version
 		dropTable(setting, "j_" + setting.inputSchema);
 		
 		// Copy the current journal
 		sql = "create table @outputSchema.@j as SELECT * FROM @outputTable";
 		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPredictor);
		fieldMap = new HashMap<>();
		fieldMap.put("@j", "j_" + setting.inputSchema);
		sql = escapeEntityMap(setting, sql, fieldMap);
		Network.executeUpdate(setting.dataSource, sql);
		
		// Log time in the database
		sql = "insert into @outputTable (schema_name, runtime) values ('" + setting.inputSchema + "', " + elapsedTime + ")";
 		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "log");
		Network.executeUpdate(setting.dataSource, sql);

		// Log the time into the local log in a nice-to-read format
		logger.debug("Time of finishing: " + LocalDate.now() + " " + LocalTime.now());
		logger.debug("Run time: " + DurationFormatUtils.formatDurationWords(elapsedTime, true, true));
 	}
 	

	// 1a) Return create journal_predictor table command
	// Return true if the journal table was successfully created.
	// Note: Default values are not supported on SAS data sets -> avoid them.
	public static boolean getJournal(Setting setting) {
		logger.debug("# Setting up journal table #");

		// The primary key is set directly behind the column name, not at the end, because SAS supports only the first declaration.
		String sql = "CREATE TABLE @outputTable ("+
	      "predictor_id " + setting.typeInteger + " NOT NULL PRIMARY KEY, " + // Let the database give the PK a unique name (that allows the user to make multiple copies of the journal)
	      "group_id " + setting.typeInteger + ", " +
	      "start_time " + setting.typeTimestamp + ", " +
	      "run_time " + setting.typeDecimal + "(18,3), " +  // Old MySQL and SQL92 do not have/require support for fractions of a second. 
	      "predictor_name " + setting.typeVarchar + "(255), " +	// In MySQL pure char is limited to 255 bytes -> stick to this value if possible
		  "predictor_long_name " + setting.typeVarchar + "(512), " +
	      "table_name " + setting.typeVarchar + "(1024), " +	// Table is a reserved keyword -> use table_name
	      "column_list " + setting.typeVarchar + "(1024), " +
	      "propagation_path " + setting.typeVarchar + "(1024), " +
	      "propagation_depth " + setting.typeInteger + ", " +	
	      "date_constrain " + setting.typeVarchar + "(255), " +
	      "parameter_list " + setting.typeVarchar + "(1024), " +
	      "pattern_name " + setting.typeVarchar + "(255), " + 
	      "pattern_author " + setting.typeVarchar + "(255), " + 
	      "pattern_code " + setting.typeVarchar + "(3600), " +	// For example code for WoE is close to 1024 chars and NB is 3000
	      "sql_code " + setting.typeVarchar + "(3600), " + // For example code for WoE is close to 1024 chars and NB is 3000
	      "target " + setting.typeVarchar + "(255), " +
	      "relevance " + setting.typeDecimal + "(18,3), " +
	      "qc_rowCount " + setting.typeInteger + ", " +
	      "qc_nullCount " + setting.typeInteger + ", " +
	      "is_ok " + setting.typeInteger + ", " +
		  "is_duplicate " + setting.typeInteger + ", " +
		  "duplicate_name " + setting.typeVarchar + "(255))";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPredictor);
		
		return Network.executeUpdate(setting.dataSource, sql);
	}
	 
	// 1b) Add record into the journal_predictor
	// Return true if the journal table was successfully updated.
	public static boolean addToJournal(Setting setting, Predictor predictor) {

		// Convert bool to int
		int isOk = predictor.isOk()? 1 : 0;
		int isInferiorDuplicate = predictor.isInferiorDuplicate()? 1 : 0;
		
		// Insert timestamp subquery
		String timestampBuild = date2query(setting, predictor.getTimestampBuilt());
		
		// Assembly the head of the insert command
        String sql = "INSERT INTO @outputTable VALUES (";
        sql = expandName(sql);
        sql = escapeEntity(setting, sql, setting.journalPredictor);

        // Add the payload (which should not be transformed)
        sql += predictor.getId() + ", " +
	        predictor.getGroupId() + ", " +
	              timestampBuild + ", " +
	              predictor.getRuntime()+ ", " +
	        "'" + predictor.getName() + "', " +
			"'" + predictor.getLongName() + "', " +
	        "'" + predictor.originalTable + "', " + 			
	        "'" + predictor.columnMap.toString() + "', " + 		// Should be a list...
	        "'" + predictor.propagationPath.toString() + "', " + 
	              predictor.propagationPath.size() + ", " + 
	        "'" + predictor.propagationDate + "', " + 
	        "'" + predictor.getParameterMap().toString() + "', " +  // Violates the 1st norm...
	        "'" + predictor.getPatternName() + "', " +
			"'" + predictor.getPatternAuthor() + "', " +
	        "'" + predictor.getPatternCode().replaceAll("'", "''") + "', " +	// Escape single quotes
	        "'" + predictor.getSql().replaceAll("'", "''") + "', " +		// Escape single quotes
			"'" + setting.targetColumn + "', " + 
			      predictor.getRelevance(setting.baseTarget) + ", " + // Chi2
	              predictor.getRowCount() + ", " + 
	              predictor.getNullCount() + ", " +
				  isOk + ", " +
				  isInferiorDuplicate + ", " +
			"'" + predictor.getDuplicateName() + "')";

		return Network.executeUpdate(setting.dataSource, sql);
	}

	public static boolean getJournalTable(Setting setting) {
		logger.debug("# Setting up journal table for propagated tables #");

		// An important limitation: Oracle limits name length of an identifier to 30 characters
		String sql = "CREATE TABLE @outputTable ("+
				"table_id " + setting.typeInteger + " NOT NULL PRIMARY KEY, " + // Let the database give the PK a unique name (that allows the user to make multiple copies of the journal)
				"start_time " + setting.typeTimestamp + ", " +
				"run_time " + setting.typeDecimal + "(18,3), " +
				"table_name " + setting.typeVarchar + "(255), " +
				"original_name " + setting.typeVarchar + "(255), " +
				"temporal_constraint " + setting.typeVarchar + "(255), " +
				"temporal_constraint_reasoning " + setting.typeVarchar + "(1024), " +
				"candidate_temporal_list " + setting.typeVarchar + "(2024), " +
				"candidate_temporal_count " + setting.typeInteger + ", " +
				"row_count_optimistic " + setting.typeInteger + ", " +
				"propagation_path " + setting.typeVarchar + "(1024), " +
				"propagation_depth " + setting.typeInteger + ", " +
				"propagation_id " + setting.typeVarchar + "(1024), " +
				"sql_code " + setting.typeVarchar + "(2024), " + // For example code for WoE is close to 1024 chars
				"is_id_unique " + setting.typeInteger + ", " +
				"is_target_id_unique " + setting.typeInteger + ", " +
				"qc_successfully_executed " + setting.typeInteger + ", " +
				"qc_row_count " + setting.typeInteger + ", " +
				"is_ok " + setting.typeInteger + ")";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalTable);

		return Network.executeUpdate(setting.dataSource, sql);
	}

	public static boolean addToJournalTable(Setting setting, MetaOutput.OutputTable table) {

		// Convert bool to int
		int isSuccessfullyExecuted = table.isSuccessfullyExecuted ? 1 : 0;
		int isOk = table.isOk ? 1 : 0;
		int isIdUnique = table.isIdUnique ? 1 : 0;
		int isUnique = table.isTargetIdUnique ? 1 : 0;

		// Convert date to query
		String timestampDesigned = date2query(setting, table.timestampDesigned);

		// Leave null unquoted. Otherwise quote.
		String temporalConstraint = table.temporalConstraint==null ? "NULL" : "'" + table.temporalConstraint + "'";


        // Assembly the head of the insert command
        String sql = "INSERT INTO @outputTable VALUES (";
        sql = expandName(sql);
        sql = escapeEntity(setting, sql, setting.journalTable);

        // Add the payload (which should not be transformed)
        sql +=  table.propagationOrder + ", " +
				timestampDesigned + ", " +
				table.timestampDesigned.until(table.timestampDelivered, ChronoUnit.MILLIS)/1000.0 + ", " +
				"'" + table.name + "', " +
				"'" + table.originalName + "', " +
				temporalConstraint + ", " +
				"'" + table.temporalConstraintJustification + "', " +
				"'" + table.getColumns(setting, StatisticalType.TEMPORAL) + "', " +
				table.getColumns(setting, StatisticalType.TEMPORAL).size() + ", " +
				table.temporalConstraintRowCountOptimistic + ", " +
				"'" + table.propagationPath.toString() + "', " +
				table.propagationPath.size() + ", " +
				"'" + table.propagationForeignConstraint.fColumn + "', " +
				"'" + table.sql.replaceAll("'", "''") + "', " +		// Escape single quotes
				isIdUnique + ", " +
				isUnique + ", " +
				isSuccessfullyExecuted + ", " +
				table.rowCount + ", " +
				isOk + ")";

		return Network.executeUpdate(setting.dataSource, sql);
	}


	public static boolean getJournalPattern(Setting setting) {
		String sql = "CREATE TABLE @outputTable (" +
				"name varchar(255) NOT NULL PRIMARY KEY, " +
				"author varchar(255), " +
				"is_aggregate integer, " +
				"is_multivariate integer, " +
				"is_nominal integer, " +
				"is_numerical integer, " +
				"is_temporal integer, " +
				"uses_base_target integer, " +
				"uses_base_date integer, " +
				"description varchar(4000))";  // Text data type would be preferred, but text is not supported on SAS. 4000 is a limit of Oracle.

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPattern);

		return Network.executeUpdate(setting.dataSource, sql);
	}

	// NOT NICE TO OPEN A CONNECTION HERE
	public static boolean addToJournalPattern(Setting setting, Collection<Predictor> predictorCollection) {

		// Initialization
		String sql = "insert into @outputTable " +
				"(name, author, is_aggregate, is_multivariate, is_nominal, is_numerical, is_temporal, uses_base_target, uses_base_date, description) " +
				"values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPattern);

		String regexPattern = "@\\w+Column\\w*";
		java.util.regex.Pattern compiledPattern = java.util.regex.Pattern.compile(regexPattern);

		try (Connection connection = setting.dataSource.getConnection();
			 PreparedStatement ps = connection.prepareStatement(sql)) {

			// Batch insert is not supported by SAS -> can't use addBatch().
			// Commit/Rollback is not supported by SAS -> can't use setAutoCommit().
			for (Predictor predictor : predictorCollection) {
				Set<String> columnSet = new HashSet<>();
				Matcher m = compiledPattern.matcher(predictor.getPatternCode());
				while (m.find()) {
					columnSet.add(m.group());
				}

				ps.setString(1, predictor.getPatternName());
				ps.setString(2, predictor.getPatternAuthor());
				ps.setInt(3, predictor.getPatternCardinality().equals("n")?1:0);
				ps.setInt(4, columnSet.size()>1?1:0);
				ps.setInt(5, contains(predictor, "@nominalColumn"));
				ps.setInt(6, contains(predictor, "@numericalColumn"));
				ps.setInt(7, contains(predictor, "@timeColumn"));
				ps.setInt(8, contains(predictor, "@baseTarget"));
				ps.setInt(9, contains(predictor, "@baseDate"));
				ps.setString(10, predictor.getPatternDescription().replaceAll(" +", " ").replaceAll("\\t", "").trim());
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	// Does the substring appear in the code or parameter of the pattern?
	private static int contains(Predictor predictor, String substring) {
		boolean result = predictor.getPatternCode().contains(substring);
		if (result) return 1;

		for (String parameter : predictor.getParameterMap().values()) {
			if (parameter.contains(substring)) return 1;
		}

		return 0;
	}


	// NOTE: IN PROGRESS OF CONSTRUCTION
	public static boolean getJournalTemporal(Setting setting) {
		logger.debug("# Setting up journal table for temporal constraints #");

		// An important limitation: Oracle limits name length of an identifier to 30 characters
		String sql = "CREATE TABLE @outputTable ("+
				"temporal_constraint_id " + setting.typeInteger + " NOT NULL PRIMARY KEY, " + // Let the database give the PK a unique name (that allows the user to make multiple copies of the journal)
				"table_name " + setting.typeVarchar + "(255), " +
				"column_name " + setting.typeVarchar + "(255), " +
				"data_type " + setting.typeVarchar + "(255), " +
				"is_target_date_defined " + setting.typeInteger + ", " +
				"is_target_id_distinct " + setting.typeInteger + ", " +
				"contains_null " + setting.typeInteger + ", " +
				"contains_future_date " + setting.typeInteger + ", " +
				"is_nullable " + setting.typeInteger + ", " +
				"optimistic_row_count " + setting.typeInteger + ", " +
				"is_temporal_constraint " + setting.typeInteger + ")";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalTemporal);

		return Network.executeUpdate(setting.dataSource, sql);
	}

	// NOTE: IN PROGRESS OF CONSTRUCTION
	public static boolean addToJournalTemporal(Setting setting, MetaOutput.OutputTable table) {


		Column column = table.getColumn("");

		// Assembly the insert
		// NOTE: batch or multiple inserts.
		String sql = "INSERT INTO @outputTable VALUES (" +
				table.name + ", " +
				column.name + ", " +
				column.dataTypeName + ", " +
				(setting.targetDate == null) + ", " +
				table.isIdUnique + ", " +
				column.containsNull(setting, table.name) + ", " +
				column.containsFutureDate(setting, table.name) + ", " +
				column.isNullable + ", " +
				0 + ", " +
				table.temporalConstraint + ")";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalTemporal);

		return Network.executeUpdate(setting.dataSource, sql);
	}


	// 2) Get base table (a table with id, targets and horizon dates).
 	// The base table could be practical, because we may simply add random sample column.
 	// Return true if the base table was successfully created.
	// IS NOT USING SYSTEM ESCAPING
 	public static boolean getBase(Setting setting) {
 		logger.debug("# Setting up base table #");
 		
		String sql;
		String id = "";
		String dateAs = "";
		String dateAsTable = "";
		String dateCondition = "";

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Detect duplicates in the base table
		boolean isTargetTupleUnique = isTargetTupleUnique(setting, setting.targetTable);

		// Use date?
		if (setting.targetDate != null) {
			dateAs = " @targetDate AS " + escapeAlias(setting, setting.baseDate) + ",";
			dateAsTable =  " t1.@targetDate AS " + escapeAlias(setting, setting.baseDate) + ",";
			dateCondition = " WHERE @targetDate IS NOT NULL";
		}

		// Deduplicate the base table if necessary
		if (isTargetTupleUnique) {
			// Prepare aliases for targetId
			for (int i = 0; i < setting.baseIdList.size(); i++) {
				id = id + QL + setting.targetIdList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseIdList.get(i)) + ", ";
			}

			// The query itself
			sql = "SELECT " + id + dateAs + " @targetColumn AS " + escapeAlias(setting, setting.baseTarget) + ", FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " FROM @targetTable" + dateCondition;
		} else {
			logger.warn("The base table contains duplicate values in {BaseID, BaseDate}. " +
					"Continuing without ALL duplicate values. " +
					"The results will be incomplete and possibly biased. " +
					"To get correct results create an artificial key / use time column with higher precision...");

			// Prepare aliases for targetId
			for (int i = 0; i < setting.baseIdList.size(); i++) {
				id = id + " t1." + QL + setting.targetIdList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseIdList.get(i)) + ",";
			}

			// The query itself (two scenarios to avoid putting everything like a puzzle)
			if (setting.targetDate == null) {
				sql = "SELECT" + id + " t1.@targetColumn AS " + escapeAlias(setting, setting.baseTarget) + ", FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " " +
						"FROM @targetTable t1 LEFT JOIN (" +
						"SELECT @targetId FROM @targetTable GROUP BY @targetId HAVING count(*)>1 " +
						") t2 " +
						"ON t1.@targetId = t2.@targetId " +
						"WHERE t2.@targetId is null";
			} else {
				sql = "SELECT" + id + dateAsTable + " t1.@targetColumn AS " + escapeAlias(setting, setting.baseTarget) + ", FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " " +
						"FROM @targetTable t1 LEFT JOIN (" +
						"SELECT @targetId, @targetDate FROM @targetTable GROUP BY @targetId, @targetDate HAVING count(*)>1 " +
						") t2 " +
						"ON t1.@targetId = t2.@targetId AND t1.@targetDate = t2.@targetDate" +
						"WHERE t2.@targetId is null AND t1.@targetDate is not null"; // TargetDate should never be null
			}
		}
		
		// Assembly the query.
		// Originally we were creating a view. But SAS does not support sampling of views. Hence, create a table.
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.baseTable);
		
		// Execute the query
		boolean isCreated = Network.executeUpdate(setting.dataSource, sql);
		
		if (!isCreated) {
			logger.warn("The base table was not successfully created.");
		}

		return (isTargetTupleUnique && isCreated);
	}
	

	// Sample base table based on target class.
	// Note: The selection is not guaranteed to be random.
	public static void getSubSampleClassification(Setting setting, SortedMap<String, Table> metaInput) {
		
		// Initialization
		String sql = "";
		Set<String> targetValueList = metaInput.get(setting.targetTable).getColumn(setting.targetColumn).uniqueValueSet;
		String quote = "";
		
		// Iff the target is nominal, quote the values with single quotes.
		if (setting.isTargetString) {
			quote = "'";
		}
		
		// Create union 
		for (String targetValue : targetValueList) {
			sql = sql + "(" + Parser.limitResultSet(setting, "SELECT * FROM @baseTable WHERE @baseTarget = " + quote + targetValue + quote + "\n", setting.sampleCount) + ")";
			sql = sql + " UNION ALL \n";    // Add "union all" between all the selects.
		}
		
		// Finally, add unclassified records.
		sql = sql + "(" + Parser.limitResultSet(setting, "SELECT * FROM @baseTable WHERE @baseTarget is null\n", setting.sampleCount) + ")";
						
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.baseSampled);
		
		Network.executeUpdate(setting.dataSource, sql);

		// Add indexes
		addIndex(setting, setting.baseSampled);
	}

	// Subsample base table.
	// Note: The selection is not guaranteed to be random.
	public static void getSubSampleRegression(Setting setting) {

		// Initialize
		String sql = Parser.limitResultSet(setting, "SELECT * FROM @baseTable", setting.sampleCount);

		// Execute
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.baseSampled);

		Network.executeUpdate(setting.dataSource, sql);

		// Add indexes
		addIndex(setting, setting.baseSampled);
	}

 	
	// 3) Propagate ID. The map should contain @outputTable, @propagatedTable, @inputTable and @targetId[?].
	// If the map contains @dateColumn, time condition is added.
	// Technical note: We have to return SQL string, because these things are logged and exported for the user
 	// as the "scoring code" for predictors.
	// IS NOT USING SYSTEM ESCAPING
	public static String propagateID(Setting setting, MetaOutput.OutputTable table){
		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape the SELECT part
		String baseId = "";
		for (String id : setting.baseIdList) {
			baseId = baseId + "t1." + QL + id + QR + ", ";
		}

		// Escape the join part
		String joinOn = "ON t1." + QL + table.propagationForeignConstraint.column.get(0) + QR + " = t2." + QL + table.propagationForeignConstraint.fColumn.get(0) + QR;
		for (int i = 1; i < table.propagationForeignConstraint.column.size(); i++) {
			joinOn += " AND t1." + QL + table.propagationForeignConstraint.column.get(i) + QR + " = t2." + QL + table.propagationForeignConstraint.fColumn.get(i) + QR;
		}

		String sql = "SELECT " + baseId +
				(setting.targetDate == null ? "" : "t1.@baseDate, ") +
				"t1.@baseTarget, " +
				"t1.@baseFold, " +
				"t2.* " + 
				"FROM @propagatedTable t1 " +
				"INNER JOIN @inputTable t2 " + joinOn;

		// Add time condition if dateColumn is present
		// The comparison "t2.@dateColumn <= t1.@baseDate" has to use <= to get 
		// the data from "the current date" when lead is 0.
		if (table.temporalConstraint !=null) {
			// First the upper bound (lead)
			sql = sql + " WHERE t2.@dateColumn <= " + setting.dateAddSyntax;
			sql = sql.replaceAll("@amount", "-" + setting.lead); // Negative lead
			
			// Then, if required, add the lower bound (lag)
			if (table.dateBottomBounded) {
				sql = sql + " AND " + setting.dateAddSyntax + " <= t2.@dateColumn";
				Integer leadLag = setting.lead + setting.lag;
				sql = sql.replaceAll("@amount", "-" + leadLag); // Negative lead+lag
			}

			sql = sql.replaceAll("@datePart", setting.unit);
		}
						
		// Pattern_code to SQL conversion
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table.name);
		sql = escapeEntityTable(setting, sql, table);
					
		return sql;
	}
	
	// 4) Get predictor
	public static String getPredictor(Setting setting, Predictor predictor){
		String sql = predictor.getSql();

		sql = Parser.expandBase(setting, sql);
		sql = Parser.expandBasePartitionBy(setting, sql);

		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, predictor.outputTable);	
		sql = escapeEntityPredictor(setting, sql, predictor);		
		
		if ("SAS".equals(setting.databaseVendor)) {
			sql = sql.replace("\"", "");				// VERY UGLY WORKAROUND. SHOULD IMPLEMENT quoteAlias
		}
		
		return sql;
	}
	
	// 5) Assembly the final step - the output table
	// Note: The current implementation stores only up to ~3600 predictors. So far the limit is acceptable as column 
	// count in a table is commonly limited (1600 columns in PostgreSQL and 1000 columns in Oracle).
	// UNSYSTEMATIC ESCAPING
	public static void getMainSample(Setting setting, Collection<Predictor> predictorList) {

		// Extract table and column names.
		// ASSUMING THAT THE MATCH IS 1:1!
		List<String> tableListAll = new ArrayList<>();
		List<String> columnListAll = new ArrayList<>();
		
		for (Predictor predictor : predictorList) {
				tableListAll.add(predictor.outputTable);
				columnListAll.add(predictor.getName());
		}
		
		// MySQL can join maximally 61 tables in a single command. Hence split the task into several smaller joins.
		List<List<String>> tableListSmall = Lists.partition(tableListAll, 60); // One table is reserved for baseSampled.
		List<List<String>> columnListSmall = Lists.partition(columnListAll, 60);
		
		// Prepare a list of the temporary table names
		List<String> tempTableList = new ArrayList<>();

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Id list
		String idList = "";
		for (String id : setting.baseIdList) {
			idList += "t1." + QL + id + QR + ", ";
		}

		// Date
		String date = "";
		if (setting.targetDate != null) {
			date = "t1.@baseDate, ";
		}
		
		// Create temporary tables
		for (int i = 0; i < tableListSmall.size(); i++) {
			// Initialization
			StringBuilder stringBuffer = new StringBuilder(500);
			int tableCount;													// The tables are named t1..t* in the join
			String tempTable = setting.mainTable + "_temp" + (100+i); 	// The name of the temporary table
			tempTableList.add(tempTable);
			List<String> tableList = tableListSmall.get(i);					
			List<String> columnList = columnListSmall.get(i);

			// Select part 
			stringBuffer.append("SELECT " + idList + date + "t1.@baseTarget");
			tableCount = 2;
			for (String column : columnList) {
				stringBuffer.append(", t" + tableCount + ".@"  + column);	// The column name will be escaped
				tableCount++;
			}
			
			// From part
			stringBuffer.append(" FROM @baseSampled t1");
			tableCount = 2;
			for (String table : tableList) {

				// Join condition
				String joinCondition = "";
				for (String id : setting.baseIdList) {
					joinCondition += "t1." + QL+ id + QR + " = t" + tableCount + "." + QL + id + QR + " AND ";
				}
				joinCondition = StringUtils.stripEnd(joinCondition, " AND ");

				stringBuffer.append(" LEFT JOIN " + table + " t" + tableCount + " ON " + joinCondition);

				if (setting.targetDate != null) {
					stringBuffer.append(" AND t1.@baseDate = t" + tableCount + ".@baseDate");
				}

				tableCount++;
			}
			
			// Make SQL from the pattern
			String pattern_code = stringBuffer.toString();
			pattern_code = addCreateTableAs(setting, pattern_code);
			pattern_code = expandName(pattern_code);
			pattern_code = expandNameList(pattern_code, tableList);
			pattern_code = escapeEntity(setting, pattern_code, tempTable); 
			
			// Escape table & column entities (tables can't be escaped in definition because they have to be first expanded...)
			Map<String, String> map = new HashMap<>(61);
			for (String table : tableList) map.put(table, table);			// This is dangerous (no prefix... in the substitution) 
			for (String column : columnList) map.put("@" + column, column);
			String sql = escapeEntityMap(setting, pattern_code, map);
			
			// Execute the query
			Network.executeUpdate(setting.dataSource, sql);
			
			// Build index on BaseId in the temporary table for possibly faster final join
			addIndex(setting, tempTable);
		}
		
		//// Combine the temporary tables into a single output table ////
		StringBuilder stringBuffer = new StringBuilder(500);
		
		// Select part (like t1.column1)
		stringBuffer.append("SELECT " + idList + date + "t1.@baseTarget");
		
		for (int i = 0; i < tempTableList.size(); i++) {
			for (String column : columnListSmall.get(i)) {
				stringBuffer.append(", t" + (i+2) + ".@"  + column);	// The column name will be escaped
			}	
		}
		
		// From part
		stringBuffer.append(" FROM @baseSampled t1");

		for (int i = 0; i < tempTableList.size(); i++) {
			int tableCount = i+2;
			String tempTable = tempTableList.get(i);

			// Join condition
			String joinCondition = "";
			for (String id : setting.baseIdList) {
				joinCondition += "t1." + QL+ id + QR + " = t" + tableCount + "." + QL + id + QR + " AND ";
			}
			joinCondition = StringUtils.stripEnd(joinCondition, " AND ");

			stringBuffer.append(" INNER JOIN " + tempTable + " t" + tableCount + " ON " + joinCondition);

			if (setting.targetDate != null) {
				stringBuffer.append(" AND t1.@baseDate = t" + tableCount + ".@baseDate");
			}
		}
		
		// Make SQL from the pattern
		String pattern_code = stringBuffer.toString();
		pattern_code = addCreateTableAs(setting, pattern_code);
		pattern_code = expandName(pattern_code);
		pattern_code = expandNameList(pattern_code, tempTableList);
		pattern_code = escapeEntity(setting, pattern_code, setting.mainTable); 
		
		// Escape table & column entities (tables can't be escaped in definition because they have to be first expanded...)
		Map<String, String> map = new HashMap<>(61);
		for (String table : tempTableList) map.put(table, table);
		for (String column : columnListAll) map.put("@" + column, column);
		String sql = escapeEntityMap(setting, pattern_code, map);
		
		// Execute the query
		Network.executeUpdate(setting.dataSource, sql);
		
		
		//// Perform output Quality Control ////
		List<String> suspiciousPatternList = qcPredictors(setting);
		if (suspiciousPatternList.size()>0) {
			logger.warn("Following patterns always failed: " + suspiciousPatternList.toString());
		}
		
		int columnCount = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable).size();
		logger.debug("Table " + setting.mainTable + " contains: " + columnCount + " columns");
	}

	// Subroutine - transform java date to SQL date
	private static String date2query(Setting setting, LocalDateTime date) {
		DateTimeFormatter formatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

		String template = setting.insertTimestampSyntax;
		String timestamp = date.format(formatter);
		timestamp = template.replace("@timestamp", timestamp);

		return timestamp;
	}

}

	