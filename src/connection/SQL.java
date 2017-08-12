package connection;

import com.google.common.collect.Lists;
import extraction.Journal;
import extraction.Predictor;
import meta.Column;
import meta.MetaOutput;
import meta.StatisticalType;
import meta.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import parser.ANTLR;
import run.Setting;
import utility.CountAppender;
import utility.Memory;
import utility.Meta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;

public class SQL {
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
	protected static String expandName(String sql) {
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
	protected static String escapeEntity(Setting setting, String sql, String outputTable) {
		// Test parameters
		if (setting.targetIdList == null || setting.targetIdList.isEmpty()) {
			throw new IllegalArgumentException("Target ID list is required");
		}
		if (setting.baseIdList.isEmpty()) {
			throw new IllegalArgumentException("Base id list is required");
		}
		if (setting.targetColumnList == null || setting.targetColumnList.isEmpty()) {
			throw new IllegalArgumentException("Target column is required");
		}
		if (setting.baseTargetList.isEmpty()) {
			throw new IllegalArgumentException("Base target list is required");
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
		if (StringUtils.isBlank(setting.baseDate)) {
			throw new IllegalArgumentException("Base date is required");
		}

		// Escape list entities
		sql = Parser.expandToList(setting, sql, "@baseId", setting.baseIdList);
		sql = Parser.expandToList(setting, sql, "@baseTarget", setting.baseTargetList);
		sql = Parser.expandToList(setting, sql, "@targetId", setting.targetIdList);
		sql = Parser.expandToList(setting, sql, "@targetColumn", setting.targetColumnList);

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape individual entities
		sql = sql.replace("@baseDate", QL + setting.baseDate + QR);
		sql = sql.replace("@baseFold", QL + setting.baseFold + QR);
		sql = sql.replace("@baseTable", QL + setting.baseTable + QR);
		sql = sql.replace("@baseSampled", QL + setting.baseSampled + QR);
		sql = sql.replace("@targetDate", QL + setting.targetDate + QR);
		sql = sql.replace("@targetTable", QL + setting.targetTable + QR);
		sql = sql.replace("@inputSchema", QL + setting.inputSchema + QR);
		sql = sql.replace("@outputSchema", QL + setting.outputSchema + QR);
		sql = sql.replace("@targetSchema", QL + setting.targetSchema + QR);
		sql = sql.replace("@outputTable", QL + outputTable + QR);

		return sql;
	}

	// Subroutine 3.2: Replace & escape the entities from predictor fields
	// Note: If a named entity (attribute/table/schema) contains single quote or a double quote, we can get into a
	// problem. The problem is explained at:
	//      https://www.owasp.org/index.php/Preventing_SQL_Injection_in_Java
	// A possible solution could be to completely switch to FluentJdbc.
	private static String escapeEntityPredictor(Setting setting, String sql, Predictor predictor) {
		// Test parameters
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("Code is required");
		}
		if (predictor.getColumnMap() == null) {
			throw new IllegalArgumentException("ColumnMap can not be null, but it can be empty");
		}
		if (StringUtils.isBlank(predictor.getPropagatedTable())) {
			throw new IllegalArgumentException("PropagatedTable is required");
		}

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Replace variables with the escaped entities
		for (String columnName : predictor.getColumnMap().keySet()) {
			sql = sql.replace(columnName, QL + predictor.getColumnMap().get(columnName) + QR);
		}

		// @columnName is replaced at the end because it may contain a substring with the name of some @variable
		sql = sql.replace("@propagatedTable", QL + predictor.getPropagatedTable() + QR);
		sql = sql.replace("@targetName", QL + predictor.getBaseTarget() + QR); // TargetName is nullable (e.g. does not depend on the target column)
		sql = sql.replace("@columnName", escapeAlias(setting, predictor.getName()));

		return sql;
	}

	// Subroutine 3.3: Replace & escape the entities from map
	// IT COULD CALL escapeEntity to avoid the necessity to call 2 different escapeEntity*
	protected static String escapeEntityMap(Setting setting, String sql, Map<String, String> fieldMap) {
		// Test parameters
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("Code is required");
		}

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape the entities
		for (Entry<String, String> field : fieldMap.entrySet()) {
			sql = sql.replace(field.getKey(), QL + field.getValue() + QR);
		}

		return sql;
	}

	protected static String escapeEntityTable(Setting setting, String sql, MetaOutput.OutputTable table) {
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

	// Back up a table in the output schema
	// Note: We copy the table instead of renaming the table because each database is using a different syntax.
	// And we already have a working procedure for creating tables from other tables.
	private static void bkpTable(Setting setting, Map<String, Table> tableMap, String tableName) {
		// Abbreviate the known names to conform the strict limit on count of chars
		String abbreviatedName = tableName.replace("journal", "j");
		abbreviatedName = abbreviatedName.replace("MAINSAMPLE", "MS");

		// Initialization
		String bkpName = setting.bkpPrefix + "_" + setting.inputSchema + "_" + abbreviatedName;

		// Conditionally drop the old back up
		if (tableMap.containsKey(bkpName)) dropTable(setting, bkpName);

		// Copy the table into a back up
		String sql = "SELECT * FROM @outputTable";

		// From table
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, tableName);

		// Into table
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, bkpName);

		Network.executeUpdate(setting.dataSource, sql);
	}

	// Prepare output schema for Predictor Factory
	// Note: The function is using names from the setting object. If the current setting doesn't match the setting
	// with witch the tables were generated, the tables are NOT going to get dropped!
	// Note: Deleting whole schema is not going to work, if it contains tables (at least in PostgreSQL).
	// Note: We could delete all tables in the schema. But I am terrified of consequences, if someone with administrator
	// privileges entered wrong output schema (for example, if someone swaps by accident input and output schema).
	// Also, if someone has set up Predictor Factory that inputSchema=outputSchema, we would delete all the user's data.
	// Note: Use addBatch() for speeding up if possible. Take care of memory limits as described at:
	//  http://viralpatel.net/blogs/batch-insert-in-java-jdbc/
	// But first check that it is going to work in SAS.
	// Note: We have to delete the tables/views in the reverse order of their creation because of dependencies if views
	// are used.
	public static void prepareOutputSchema(Setting setting) {
		// Get list of tables
		Map<String, Table> tableMap = Meta.collectTables(setting, setting.database, setting.outputSchema);

		// Create tables if they do not exist
		if (!tableMap.containsKey(setting.journalRun)) getJournalRun(setting);

		// Back up by replacing the old back up
		if (tableMap.containsKey(setting.mainTablePrefix)) bkpTable(setting, tableMap, setting.mainTablePrefix);
		if (tableMap.containsKey(setting.journalPattern)) bkpTable(setting, tableMap, setting.journalPattern);
		if (tableMap.containsKey(setting.journalPredictor)) bkpTable(setting, tableMap, setting.journalPredictor);
		if (tableMap.containsKey(setting.journalTable)) bkpTable(setting, tableMap, setting.journalTable);
		if (tableMap.containsKey(setting.journalTemporal)) bkpTable(setting, tableMap, setting.journalTemporal);

		// Select tables for dropping
		SortedMap<String, String> dropMap = new TreeMap<>();
		for (String table : tableMap.keySet()) {
			if (table.startsWith(setting.mainTablePrefix)) dropMap.put(1 + table, table);         // MainSample and it's temporary tables
			if (table.startsWith(setting.predictorPrefix)) dropMap.put(2 + table, table);   // Predictors
			if (table.startsWith(setting.propagatedPrefix)) dropMap.put(3 + table, table);  // Propagated tables
			if (table.equals(setting.baseSampled)) dropMap.put(4 + table, table);           // Sampled base table
			if (table.equals(setting.baseTable)) dropMap.put(5 + table, table);             // Base table
			if (table.equals(setting.journalPattern)) dropMap.put(6 + table, table);        // Journal patterns
			if (table.equals(setting.journalPredictor)) dropMap.put(7 + table, table);      // Journal table
			if (table.equals(setting.journalTable)) dropMap.put(8 + table, table);          // Journal propagated table
			if (table.equals(setting.journalTemporal)) dropMap.put(9 + table, table);       // Journal temporal constrains
		}

		// Drop the tables
		for (String table : dropMap.values()) {
			dropTable(setting, table);
		}

		// Create tables unique necessary for this run
		getJournalTable(setting);
	}

	// Create index on {baseId, baseDate}.
	// Returns true if the update was successful.
	// Note: The index can not be unique because we are creating it on propagated tables (like table of transactions...).
	// Design note: There are troubles with obligatory index names (PostgreSQL allows to omit the name but other
	// databases like MySQL, Oracle, SAS or MSSQL require the index name) :
	//  1) The created index name can be already taken (e.g.: a user may have backed up the table from a previous run).
	//  2) The created index name can be too long. We could have truncated the index names, but then we could end up with duplicates.
	//  3) Naming conventions differ database from database. E.g. in SAS:
	//      For a simple index, the index name must be the same as the column name.
	//      Literature: http://support.sas.com/documentation/cdl/en/proc/61895/HTML/default/viewer.htm#a002473673.htm
	//     But PostgreSQL requires a unique index name per schema -> collisions could then happen.
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
			name = "ix" + outputTable.substring(setting.propagatedPrefix.length(), outputTable.length());
		} else if (outputTable.startsWith(setting.mainTablePrefix)) {
			name = "ix" + outputTable.substring(setting.mainTablePrefix.length(), outputTable.length());
		}

		// Table names can use ridiculous symbols like "-" or spaces. Hence we must quote the index name.
		name = escapeEntity(setting, name);

		String sql = "CREATE INDEX " + name + " ON @outputTable " + columns;

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);

		boolean isOK = Network.executeUpdate(setting.dataSource, sql);

		return isOK;
	}

	// Set primary key for a table in the output schema.
	// Design note: On Azure it is necessary to first set the column as not null. Hence, we first set not-null constraint.
	// Design note: There are generally just two ways how to create a table with a primary key constraint:
	//  1) create table with primary key first, and use SELECT into later
	//  2) create table as first, and use add primary key later
	// Of the two options, creating the primary key (and its associated index) after data is loaded will probably be faster.
	// Alternatives in MySQL, Snowflake,... include CREATE TABLE LIKE. But that does not permit modification
	// of the table content.
	public static boolean setPrimaryKey(Setting setting, String outputTable) {
		String columns = "(@baseId)";
		if (setting.targetDate != null) {
			columns = "(@baseId, @baseDate)";
		}

		// Unique constraint first, but not on Oracle because it would result into:
		//      ORA-02261: such unique or primary key already exists in the table
		// error.
		// Azure also requires Not-Null constraint. But to set the constraint we have to repeat the data type...
		// Hence, just skip creation of unique constraint.
//      String sql = "ALTER TABLE @outputTable ADD UNIQUE " + columns;
//      sql = expandName(sql);
//      sql = escapeEntity(setting, sql, outputTable);
//      Network.executeUpdate(setting.dataSource, sql);

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
		return (int) Double.parseDouble(resultList.get(0)); // SAS can return 682.0. SHOULD IMPLEMENT LIST<INTEGERS>.
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

		return (int) Double.parseDouble(resultList.get(0)); // SAS can return 682.0
	}

	// Returns true if the column contains null.
	// Overwritten in SQLOracle.
	public boolean containsNull(Setting setting, String table, String column) {

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

	// Returns true if the date column contains a date from the future.
	// NOTE: May fail on a timestamp or different database dialect.
	// Overwritten in SQLOracle.
	public boolean containsFutureDate(Setting setting, String table, String column) {

		// The good thing on "current_date" is that it is a standard. The bad thing is that it does not work in MSSQL.
		// Hence we use ODBC standard of "{fn NOW()}", which should be automatically replaced in the JDBC driver to the
		// database specific command. The ODBC command works in PostgreSQL.
		// Plus the comparison works with: date, timestamp and datetime.
		// Comparison with time fails.
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

	// Get count of tuples fulfilling the time constraint.
	// This is an optimistic estimate based on max(targetDate) and min(targetDate).
	// NOTE: It may fail on Oracle if we use month unit because we are using "interval".
	// If we used add_month, it would work.
	public static int countUsableDates(Setting setting, String table, String column) {

		// First the upper bound (lead)
		String timeConstraint = " dateToNumber(" + setting.dateAddSyntax + ") <= " + setting.baseDateRange.get(1);
		timeConstraint = timeConstraint.replaceAll("@amount", setting.lead.toString());

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
		return (int) Double.parseDouble(resultList.get(0)); // SAS can return 682.0
	}


	// Get the maximal cardinality of the table in respect to targetId. If the cardinality is 1:1,
	// we may want to remove the bottom time constrain in base propagation.
	// Note that we are working with the input tables -> alter commands are forbidden.
	// IS NOT USING SYSTEM ESCAPING
	// Overwritten in SQLOracle.
	public boolean isIdUnique(Setting setting, MetaOutput.OutputTable table) {

		// Note: Following query, which does not use exist, is ~twice as fast on unique columns but ~twice as slow
		// non-unique columns on PostgreSQL:
		//  select 1
		//  from loan
		//  having count(DISTINCT account_id) = count(account_id)
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

		if (result)
			logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " doesn't contain duplicates #");
		else
			logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " CONTAINS duplicates #");

		return result;
	}

	// Check whether the columns {baseId, baseDate} are unique in the table in the inputSchema.
	// Overwritten in SQLOracle.
	public boolean isTargetTupleUnique(Setting setting, String table) {
		// We could have used possibly faster: "ALTER TABLE @outputTable ADD UNIQUE (@baseId, @baseDate)".
		// But it would not work in Netezza as Netezza doesn't support constraint checking and referential integrity.
		String sql = "SELECT exists(" +
				"SELECT @targetId " +
				"FROM @targetTable " +
				"GROUP BY @targetId" + (setting.targetDate == null ? " " : ", @targetDate ") +
				"HAVING count(*)>1" +
				")";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return !Network.isTrue(setting.dataSource, sql); // Return negation
	}

	// Returns 1 if the baseId in the table in the outputSchema is unique.
	// It appears this can be disk space demanding (tested in Accidents dataset)
	// Overwritten in SQLOracle.
	public boolean isTargetIdUnique(Setting setting, String table) {

		String sql = "SELECT exists(" +
				"SELECT @baseId FROM @outputTable GROUP BY @baseId HAVING count(*)>1)";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return !Network.isTrue(setting.dataSource, sql); // Return negation
	}

	// Return a pivot date for concept drift detection.
	// The implementation returns the mid-range.
	// The returned value should is a temporal unit (not a number). There are two reasons:
	//  1) It is human readable
	//  2) If we are comparing date-to-date it is faster than number-to-date (we can at least use indexes)
	// The temporal logic is done in Java for portability across databases.
	// NOTE: If min==max, return warning?
	// NOTE: Branch for SAS.
	// Note: Mid-ranges for dates like 1996-03-21 23:30:00 are expected as Java is taking into account daylight changes.
	// Note: MidRange is sensitive to outlier values. Used to cause problems in VOC dataset before the data were fixed.
	public static Timestamp getPivotDate(Setting setting, int dateDataType) {
		// SAS requires informat. NOTE: NOT TESTED ON DATETIME AND TIME!
		String informat = "";
		if ("SAS".equals(setting.databaseVendor)) {
			if (dateDataType == 91) informat = " format=ddmmyy10. ";      // Date
			if (dateDataType == 92) informat = " format=time10. ";        // Time
			if (dateDataType == 93) informat = " format=datetime21. ";    // Datetime
		}

		String sql = "select min(@targetDate)" + informat +
				"     , max(@targetDate) " + informat +
				"from @targetTable";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "ignored");

		List<Timestamp> timestamps = Network.getTimestamp(setting.dataSource, sql);

		// Mid-range calculation
		Timestamp min = timestamps.get(0);
		Timestamp max = timestamps.get(1);
		Long midRangeValue = min.getTime() + (max.getTime() - min.getTime()) / 2;
		Timestamp midRange = Timestamp.from(Instant.ofEpochMilli(midRangeValue));

		logger.debug("TargetDate is in range: " + min + " to " + max + " with " + midRange + " mid-range.");

		return midRange;
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
		Map<String, String> map = new HashMap<>();
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

		// A query without an aggregate function in ORDER BY clause because of the following limitation of SAS:
		//  Summary functions are restricted to the SELECT and HAVING clauses only...
		// If desirable, wrap it in another select and return only @columnName.
		String sql = "SELECT @columnName, count(*) " +
				"FROM " + table + " " +
				"WHERE @columnName is not null " +
				"GROUP BY @columnName " +
				"ORDER BY 2 DESC";

		sql = Parser.limitResultSet(setting, sql, setting.valueCount); // Possibly not necessary...
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, tableName);
		Map<String, String> map = new HashMap<>();
		map.put("@columnName", columnName);
		map.put("@inputTable", tableName); // To cover the scenario that it's in the input schema
		sql = escapeEntityMap(setting, sql, map);

		return Network.executeQuery(setting.dataSource, sql, setting.valueCount); // Because of SAS we use JDBC maxRows
	}

	// Could the two columns in the table describe a symmetric relation (like in borderLength(c1, c2))?
	// DEVELOPMENTAL AND LIKELY USELESS...
	public static boolean isSymmetric(Setting setting, Map<String, String> map) {
		String sql = "SELECT exists("
				+ "SELECT @lagColumn, @column FROM @inputTable "
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
	public static double getR2(Setting setting, Predictor predictor, String baseTarget) {
		// Initialization
		String sql;

		// Is the predictor categorical, numeric or time?
		if ("nominal".equals(predictor.getDataTypeCategory())) {
			sql = "SELECT count(*)*power(corr(t2.average, t1.@baseTarget), 2) " +
					"FROM @outputTable t1 " +
					"JOIN ( " +
					"SELECT @column " +
					", cast(avg(@baseTarget) as decimal(38, 10)) as average " + // NOT NICE, but solves frequent arithmetic errors on MSSQL.
					"FROM @outputTable " +
					"GROUP BY @column " +
					") t2 " +
					"ON t1.@column = t2.@column " +
					"where t1.@column is not null AND t1.@baseTarget is not null";
		} else if ("numerical".equals(predictor.getDataTypeCategory())) {
			sql = "SELECT count(@column)*power(corr(@column, @baseTarget), 2) " +
					"FROM @outputTable " + /* We are working with the outputTable, hence schema & database are output. */
					"WHERE @column is not null AND @baseTarget is not null";
		} else {
			sql = "SELECT count(@column)*power(corr(dateToNumber(@column), @baseTarget), 2) " +
					"FROM @outputTable " +
					"WHERE @column is not null AND @baseTarget is not null";
		}

		// Replace the generic corr command with the database specific version.
		// Also take care of dateToNumber, stdDev (needed if the corr command is assembled from the basic commands)
		// and nullIf (SAS does not implement is).
		sql = Parser.getDialectCode(setting, sql);

		// Expand
		sql = expandName(sql);

		// Escape the entities (baseTarget must always be a single column -> escapeEntity is after escapeEntityMap)
		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", predictor.getName());
		fieldMap.put("@baseTarget", baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);
		sql = escapeEntity(setting, sql, predictor.getOutputTable());    /* outputTable, outputSchema, output.Database */

		// Execute the SQL
		List<String> response = Network.executeQuery(setting.dataSource, sql);

		// Parse the response
		double correlation;

		try {
			correlation = Double.parseDouble(response.get(0));
		} catch (Exception ignored) {
			logger.info("The response in correlation calculation is null (constant values, empty table...), return 0.");
			correlation = 0;
		}

		return correlation;
	}

	// Get Chi2.
	// Note: For maintenance purpose, the query should be decomposed into subqueries
	// that are put together with String concatenation (necessary for databases without "with" clause support).
	// The calculation could be streamlined as described in:
	//  Study of feature selection algorithms for text-categorization.
	// SHOULD BE EXTENDED TO SUPPORT BOOLEANS
	public static double getChi2(Setting setting, Predictor predictor, String baseTarget) {
		// Initialization
		String sql;


		// Technical: MySQL or PostgreSQL require correlation name (also known as alias) for derived tables.
		// But Oracle does not accept "as" keyword in front of the table alias. See:
		//   http://www.techonthenet.com/oracle/alias.php
		// Hence use following syntax: (SELECT col1 from table t1) t2.
		// Technical: Oracle and MSSQL do not permit aliases of generated attributes in ORDER BY clause. See:
		//   http://stackoverflow.com/questions/497241/how-do-i-perform-a-group-by-on-an-aliased-column-in-ms-sql-server
		//   http://stackoverflow.com/questions/2681494/why-doesnt-oracle-sql-allow-us-to-use-column-aliases-in-group-by-clauses
		// However, SAS does not return expected results if calculations happen in GROUP BY clause. The warning is:
		//   The query requires remerging summary statistics back with the original data.
		// The solution is to create a subqury, which returns table with the generated attribute. And then group the
		// table by the attribute (applies to "bin" attribute).
		// Technical: Linear regularization is appropriate assuming infinite samples. On finite samples it is possibly
		// too harsh on attributes with too many unique values. But I can live with that.
		// NOTE: We should treat NULL values as another category in the histograms/list of categorical values.
		// Technical: Azure cloud may require after us to cast some of the count(*) to decimal. Not tested.
		if ("nominal".equals(predictor.getDataTypeCategory())) {
			// Use categorical column directly
			sql = "SELECT sum(chi2)/count(distinct(bin)) " // Linearly regularized against columns with high cardinality
					+ "FROM ( "
					+ " SELECT (expected.expected-coalesce(measured.count, 0)) * (expected.expected-coalesce(measured.count, 0)) / expected.expected AS chi2"
					+ " , expected.bin AS bin"
					+ " FROM ( "
					+ "     SELECT expected_bin.count*expected_target.prob AS expected "
					+ "          , bin "
					+ "          , target "
					+ "     FROM ( "
					+ "         SELECT @baseTarget AS target "
					+ "              , 1.0 * count(*)/max(t2.nrow) AS prob " // We use 1.0 to cast integer to double (important on MSSQL)
					+ "         FROM @outputTable, ( "
					+ "             SELECT count(*) AS nrow "
					+ "             FROM @outputTable "
					+ "         ) t2 "
					+ "         GROUP BY @baseTarget "
					+ "     ) expected_target, ( "
					+ "         SELECT count(*) AS count "
					+ "              , @column AS bin "
					+ "         FROM @outputTable "
					+ "         GROUP BY @column "
					+ "     ) expected_bin "
					+ " ) expected "
					+ " LEFT JOIN ( "   // Some combinations of {@column, @baseTarget} may not be in the data -> left join
					+ "     SELECT @baseTarget AS target "
					+ "          , count(*) AS count "
					+ "          , @column AS bin "
					+ "     FROM @outputTable "
					+ "     GROUP BY @column, @baseTarget "
					+ " ) measured "
					+ " ON expected.bin = measured.bin "
					+ " AND expected.target = measured.target "
					+ ") chi2";
		} else {
			// Group numerical/time values into 10 bins.
			// If desirable you can optimize the optimal amount of bins with Sturge's rule
			// but syntax for log is different in each database.
			// There are different approaches to binning in SQL. A nice treatment of the maximum is at:
			//      https://sqlsunday.com/2014/09/14/histogram-in-t-sql/
			// Also check:
			//      https://technet.microsoft.com/en-us/library/aa224898(v=sql.80).aspx
			sql = "SELECT sum(chi2)/10 " // To match regularization of nominal columns
					+ "FROM ( "
					+ " SELECT (expected.expected-coalesce(measured.count, 0)) * (expected.expected-coalesce(measured.count, 0)) / expected.expected AS chi2 "
					+ " FROM ( "
					+ "     SELECT expected_bin.count*expected_target.prob AS expected "
					+ "          , bin "
					+ "          , target "
					+ "     FROM ( "
					+ "         SELECT @baseTarget AS target "
					+ "              , 1.0 * count(*)/max(t2.nrow) AS prob "  // We use 1.0 to cast integer to double (important on MSSQL)
					+ "         FROM @outputTable, ( "
					+ "             SELECT count(*) AS nrow "
					+ "             FROM @outputTable "
					+ "         ) t2 "
					+ "         GROUP BY @baseTarget "
					+ "     ) expected_target, ( "
					+ "         SELECT bin "
					+ "              , count(*) AS count "
					+ "         FROM ( "
					+ "             SELECT floor((@column-t2.min_value) / t2.bin_width) AS bin "
					+ "             FROM @outputTable, ( "
					+ "                 SELECT ((max(@column)-min(@column)) / 10.0) + 0.0000001 bin_width " // 0.0000001 because the maximum should fall into into the 10th bin, not the 11th.
					+ "                      , min(@column) AS min_value "                                  // Also it prevents division by zero if all the values are constant.
					+ "                 FROM @outputTable "                                                 // 10.0 because bin_width may not be a whole number.
					+ "             ) t2 "
					+ "         ) t3 "
					+ "         GROUP BY bin "
					+ "     ) expected_bin "
					+ " ) expected "
					+ " LEFT JOIN ( "
					+ "     SELECT target "
					+ "          , bin "
					+ "          , count(*) AS count "
					+ "     FROM ( "
					+ "         SELECT @baseTarget AS target "
					+ "                , floor((@column-t2.min_value) / t2.bin_width) AS bin "
					+ "         FROM @outputTable, ( "
					+ "             SELECT ((max(@column)-min(@column)) / 10.0) + 0.0000001 AS bin_width "
					+ "                  , min(@column) AS min_value "
					+ "             FROM @outputTable "
					+ "         ) t2 "
					+ "     ) t3 "
					+ "     GROUP BY bin, target "
					+ " ) measured "
					+ " ON expected.bin = measured.bin "
					+ " AND expected.target = measured.target "
					+ ") chi2";

			// For time columns just cast time to number.
			if ("temporal".equals(predictor.getDataTypeCategory())) {
				sql = sql.replace("@column", setting.dateToNumber);
			}
		}

		// Expand
		sql = expandName(sql);

		// Escape the entities
		// Since @baseTarget MUST always be a single column, escapeEntityMap() is before escapeEntity().
		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", predictor.getName());
		fieldMap.put("@baseTarget", baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);
		sql = escapeEntity(setting, sql, predictor.getOutputTable());    /* outputTable, outputSchema, output.Database */

		// Execute the SQL
		List<String> response = Network.executeQuery(setting.dataSource, sql);

		// Parse the response.
		double chi2;

		try {
			chi2 = Double.parseDouble(response.get(0));
		} catch (Exception ignored) {  // Cover's both, number format exception and null pointer exception.
			chi2 = 0;
			logger.info("The result of Chi2 calculation on " + predictor.getOutputTable() + "." + predictor.getName() + " is null (empty table...). Returning 0.");
		}

		return chi2;

	}

	// Estimate true concept drift.
	// Requires @baseFold in the target OR I need a median value for training/testing split based on the time.
	// Note: We should never really return zero to void the effect of Chi2.
	// Note: Concept drift is so far just for nominal labels. Should be extended to continuous label
	// Note: If the targetDate is a constant, we should not perform concept drift (at least not by time).
	// THIS IS POSTGRESQL DIALECT (FAILS ON ORACLE, SAS, MySQL).
	// NOTE: We should reuse the histogram calculated for Chi2 -> Proposal: Calculate histograms in SQL, send
	// them to Predictor Factory (it is necessary to limit the length of the histogram for nominal attributes)
	// and calculate Chi2 in Concept Drift in Java? This way we could also easily calculate CFS...
	// Note: Calculate it only if Chi2 is big enough to get into the output table.
	public static double getConceptDriftPostgre(Setting setting, Predictor predictor, String baseTarget) {

		String sql;

		// Nominal
		if ("nominal".equals(predictor.getDataTypeCategory())) {
			sql = "with histogram as ( " +    //  Count() by y,x,fold
					"    select @baseTarget " +
					"        , @column " +
					"        , is_testing " +
					"        , COALESCE(cnt, 0) as cnt " +
					"    from ( " +
					"        select *  " +      // The planner on PostgreSQL likes if we first get the unique values and only then perform the join.
					"        from ( " +
					"            select distinct @baseTarget " +
					"            from @outputTable " +
					"        ) t1, ( " +
					"            select distinct @column " +
					"            from @outputTable " +
					"        ) t2, ( " +
					"            select TRUE as is_testing union select FALSE as is_testing " +
					"        ) t3 " +
					"    ) t4 " +
					"    left join  " +
					"    ( " +
					"        select @baseTarget " +
					"             , @column " +
					"             , @baseDate > @pivotDate as is_testing " +
					"             , count(*) as cnt " +
					"        from @outputTable t3 " +
					"        GROUP BY @baseTarget, @column, @baseDate > @pivotDate " +
					"    ) t5 " +
					"    using(@baseTarget, @column, is_testing) " +
					"), " +
					"x as (  " +            // Counts for the attribute x
					"    select @column " +
					"         , is_testing " +
					"         , sum(cnt) as x_cnt " +
					"    from histogram " +
					"    GROUP BY @column, is_testing " +
					"), " +
					"posterior as ( " +     // Conditional probability p(y|x)
					"    select @baseTarget " +
					"        , @column " +
					"        , is_testing " +
					"        , cnt/(x_cnt+1.0) as probability " + // Laplace correction for division by zero. Avoid integer division.
					"    from histogram  " +
					"    join x " +
					"    using(@column, is_testing) " +
					"),  " +
					"minmax as ( " +
					"    select min(probability) as minimum " +
					"        , max(probability) as maximum " +
					"    from posterior " +
					"    GROUP BY @baseTarget, @column " +
					") " +
					"select sum(minimum)/sum(maximum) as jaccard_index " + // The problem is that 0 does not correspond to the random distribution; Ruzicka 1958. But if combined with CHi2, this adjustment is not necessary, as Chi2 includes correction for observation by chance.
					"from minmax";
		} else {
			sql = "with histogram as ( " +
					"    select @baseTarget " +
					"        , bin " +
					"        , is_testing " +
					"        , COALESCE(cnt, 0) as cnt " +
					"    from ( " +
					"        select *  " +  // The planner on PostgreSQL likes if we first get the unique values and only then perform the join.
					"        from ( " +
					"            select distinct @baseTarget " +
					"            from @outputTable " +
					"        ) t1, ( " +
					"            select 0 as bin union all select 1 as bin union all select 2 as bin union all select 3 as bin union all select 4 as bin  " +
					"            union all " +
					"            select 5 as bin union all select 6 as bin union all select 7 as bin union all select 8 as bin union all select 9 as bin " +
					"        ) t2, ( " +
					"            select TRUE as is_testing union all select FALSE as is_testing " +
					"        ) t3 " +
					"    ) t4 " +
					"    left join  " +
					"    ( " +
					"        SELECT @baseTarget " +
					"             , bin " +
					"             , @baseDate > @pivotDate as is_testing " +
					"             , count(*) AS cnt " +
					"        FROM ( " +
					"             SELECT @baseTarget " +
					"                  , @baseDate " +
					"                 , @column " +
					"                  , floor((@column-t2.min_value) / t2.bin_width ) AS bin " +
					"             FROM @outputTable, ( " +
					"                     SELECT ((max(@column)-min(@column)) / 10.0) + 0.00000001 AS bin_width " +
					"                          , min(@column) AS min_value " +
					"                     FROM @outputTable " +
					"             ) t2 " +
					"        ) t3 " +
					"        GROUP BY bin, @baseTarget, @baseDate > @pivotDate  " +
					"    ) t5 " +
					"    using(@baseTarget, bin, is_testing) " +
					"), " +
					"x as ( " +
					"    select bin " +
					"         , is_testing " +
					"         , sum(cnt) as x_cnt " +
					"    from histogram " +
					"    GROUP BY bin, is_testing " +
					"), " +
					"posterior as ( " +
					"    select @baseTarget " +
					"        , bin " +
					"        , is_testing " +
					"        , cnt/(x_cnt+1.0) as probability " + // Laplace correction for division by zero. Avoid integer division.
					"    from histogram  " +
					"    join x " +
					"    using(bin, is_testing) " +
					"),  " +
					"minmax as ( " +
					"    select min(probability) as minimum " +
					"        , max(probability) as maximum " +
					"    from posterior " +
					"    GROUP BY @baseTarget, bin " +
					") " +
					"select sum(minimum)/sum(maximum) as jaccard_index " +
					"from minmax ";

			// For time columns just cast time to number.
			if ("temporal".equals(predictor.getDataTypeCategory())) {
				sql = sql.replace("@column", setting.dateToNumber);
			}
		}

		// Replace, but do not escape
		sql = sql.replace("@pivotDate", "{ts '" + setting.pivotDate + "'}");

		// Expand
		sql = expandName(sql);

		// Escape the entities
		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", predictor.getName());
		fieldMap.put("@baseTarget", baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);
		sql = escapeEntity(setting, sql, predictor.getOutputTable());

		// Execute the SQL
		List<String> response = Network.executeQuery(setting.dataSource, sql);

		// Parse the response.
		double similarity;

		try {
			similarity = Double.parseDouble(response.get(0));
		} catch (Exception ignored) {  // Cover's both, number format exception and null pointer exception.
			similarity = 0;
			logger.info("The estimated concept drift on " + predictor.getOutputTable() + "." + predictor.getName() + " is null (empty table...). Returning 0.");
		}

		return similarity;
	}


	// Estimate true concept drift.
	// Requires @baseFold in the target OR I need a median value for training/testing split based on the time.
	// Note: We should never really return zero to void the effect of Chi2.
	// Note: Concept drift is so far just for nominal labels. Should be extended to continuous label
	// Note: If the targetDate is a constant, we should not perform concept drift (at least not by time).
	// NOTE: THIS IMPLEMENTATION INCREASES RUNTIME BY ~30%
	public static double getConceptDrift(Setting setting, Predictor predictor, String baseTarget) {

		String fromDual = "";
		if ("Oracle".equals(setting.databaseVendor)) fromDual = " from dual ";
		if ("SAS".equals(setting.databaseVendor)) fromDual = " from dictionary.libnames where libname = 'WORK' ";

		String histogram;

		// Nominal
		if ("nominal".equals(predictor.getDataTypeCategory())) {

			histogram = " ( " +    //  Count() by y,x,fold
					"    select t4.@baseTarget " +
					"        , t4.@column as bin " + // To make it compatible with numerical histogram we rename the column
					"        , t4.is_testing " +
					"        , COALESCE(cnt, 0) as cnt " +
					"    from ( " +
					"        select *  " +      // The planner on PostgreSQL likes if we first get the unique values and only then perform the join.
					"        from ( " +
					"            select distinct @baseTarget " +
					"            from @outputTable " +
					"        ) t1, ( " +
					"            select distinct @column " +
					"            from @outputTable " +
					"        ) t2, ( " +
					"            select 1 as is_testing " + fromDual + " union select 0 as is_testing " + fromDual + // We can't use TRUE/ELSE because Oracle does not know booleans
					"        ) t3 " +
					"    ) t4 " +
					"    left join  " +
					"    ( " +
					"       select @baseTarget " +
					"           , @column " +
					"           , is_testing " +
					"           , count(*) as cnt " +
					"       from ( " +
					"           select @baseTarget " +    // We group by a created column and SAS dislikes these things -> we first create a table
					"               , @column " +
					"               , case when (@baseDate > @pivotDate) then 1 else 0 end as is_testing " +
					"           from @outputTable " +
					"       ) t5 " +
					"       GROUP BY @baseTarget, @column, is_testing " +
					"    ) t6 " +
					"    on t4.@baseTarget=t6.@baseTarget and t4.@column=t6.@column and t4.is_testing=t6.is_testing " +
					") histogram ";

		} else {

			histogram = " ( " +
					"    select t4.@baseTarget " +
					"        , t4.bin " +
					"        , t4.is_testing " +
					"        , COALESCE(cnt, 0) as cnt " +
					"    from ( " +
					"        select *  " +  // The planner on PostgreSQL likes if we first get the unique values and only then perform the join.
					"        from ( " +
					"            select distinct @baseTarget " +
					"            from @outputTable " +
					"        ) t1, ( " +
					"            select 0 as bin " + fromDual + " union all select 1 as bin " + fromDual + " union all select 2 as bin " + fromDual + " union all select 3 as bin " + fromDual + " union all select 4 as bin  " + fromDual +
					"            union all " +
					"            select 5 as bin " + fromDual + " union all select 6 as bin " + fromDual + " union all select 7 as bin " + fromDual + " union all select 8 as bin " + fromDual + " union all select 9 as bin " + fromDual +
					"        ) t2, ( " +
					"            select 1 as is_testing " + fromDual + " union all select 0 as is_testing " + fromDual +
					"        ) t3 " +
					"    ) t4 " +
					"    left join  " +
					"    ( " +
					"     SELECT @baseTarget, bin, is_testing, count(*) AS cnt " +
					"     FROM (" +
					"         SELECT @baseTarget " +
					"              , bin " +
					"              , case when (@baseDate > @pivotDate) then 1 else 0 end as is_testing " +
					"         FROM ( " +
					"                 SELECT @baseTarget " +
					"                      , @baseDate " +
					"                      , floor((@column-t2.min_value) / t2.bin_width ) AS bin " +
					"                 FROM @outputTable, ( " +
					"                         SELECT ((max(@column)-min(@column)) / 10.0) + 0.00000001 AS bin_width " +
					"                              , min(@column) AS min_value " +
					"                      FROM @outputTable " +
					"                 ) t2 " +
					"             ) t3 " +
					"         ) t4 " +
					"         GROUP BY bin, @baseTarget, is_testing  " +
					"    ) t5 " +
					"    on t4.@baseTarget=t5.@baseTarget and t4.bin=t5.bin and t4.is_testing=t5.is_testing " +
					") histogram ";

			// For time columns just cast time to number.
			if ("temporal".equals(predictor.getDataTypeCategory())) {
				histogram = histogram.replace("@column", setting.dateToNumber);
			}
		}

		String x = " ( " +            // Counts for the attribute x
				"    select bin " +
				"         , is_testing " +
				"         , sum(cnt) as x_cnt " +
				"    from " + histogram +
				"    GROUP BY bin, is_testing " +
				") x ";

		String posterior = " ( " +     // Conditional probability p(y|x)
				"    select histogram.@baseTarget " +
				"        , histogram.bin " +
				"        , histogram.is_testing " +
				"        , cnt/(x_cnt+1.0) as probability " + // Laplace correction for division by zero. Avoid integer division.
				"    from " + histogram +
				"    join " + x +
				"    on histogram.bin=x.bin and histogram.is_testing=x.is_testing " +
				") posterior ";

		String minmax = " ( " +
				"    select min(probability) as minimum " +
				"        , max(probability) as maximum " +
				"    from " + posterior +
				"    GROUP BY @baseTarget, bin " +
				") minmax ";

		String sql =
				"select sum(minimum)/sum(maximum) as jaccard_index " + // The problem is that 0 does not correspond to the random distribution; Ruzicka 1958. But if combined with CHi2, this adjustment is not necessary, as Chi2 includes correction for observation by chance.
						"from " + minmax;

		// Replace, but do not escape
		sql = sql.replace("@pivotDate", "{ts '" + setting.pivotDate + "'}");

		// Expand
		sql = expandName(sql);

		// Escape the entities
		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", predictor.getName());
		fieldMap.put("@baseTarget", baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);
		sql = escapeEntity(setting, sql, predictor.getOutputTable());

		// Execute the SQL
		List<String> response = Network.executeQuery(setting.dataSource, sql);

		// Parse the response.
		double similarity;

		try {
			similarity = Double.parseDouble(response.get(0));
		} catch (Exception ignored) {  // Cover's both, number format exception and null pointer exception.
			similarity = 0;
			logger.info("The estimated concept drift on " + predictor.getOutputTable() + "." + predictor.getName() + " is null (empty table...). Returning 0.");
		}

		return similarity;
	}


	// QC patterns based on produced predictors
	private static List<String> qcPredictors(Setting setting) {
		String sql = "SELECT pattern_name " +
				"FROM @outputTable " +
				"GROUP BY pattern_name " +
				"having avg(is_ok + 0.0) = 0"; // We have to cast int to double before averaging

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPredictor);
		List<String> badPatterns = Network.executeQuery(setting.dataSource, sql);

		return badPatterns;
	}


	public static boolean getJournalRun(Setting setting) {
		logger.debug("# Setting up journal table for runtime summary #");

		// An important limitation: Oracle limits name length of an identifier to 30 characters
		String sql = "CREATE TABLE @outputTable (" +
				"finish_time " + setting.typeTimestamp + " PRIMARY KEY, " + // Let the database give the PK a unique name (that allows the user to make multiple copies of the journal)
				"schema_name " + setting.typeVarchar + "(255), " +
				"run_time " + setting.typeDecimal + "(18,3), " +
				"memory " + setting.typeDecimal + "(18,3), " +
				"warn_count " + setting.typeInteger + ", " +
				"error_count " + setting.typeInteger + ", " +
				"predictor_count " + setting.typeInteger + ", " +
				"predictor_output_count " + setting.typeInteger + ", " +
				"propagated_table_count " + setting.typeInteger + ", " +
				"setting " + setting.typeVarchar + "(2024), " +

				"accuracy_avg " + setting.typeDecimal + "(8,3), " +     // Should possibly create a new table [measure, value, std]
				"accuracy_std " + setting.typeDecimal + "(8,3), " +
				"auc_avg " + setting.typeDecimal + "(8,3), " +
				"auc_std " + setting.typeDecimal + "(8,3), " +
				"fscore_avg " + setting.typeDecimal + "(8,3), " +
				"fscore_std " + setting.typeDecimal + "(8,3), " +
				"precision_avg " + setting.typeDecimal + "(8,3), " +    // Precision is a reserved word in MySQL -> _avg suffix
				"precision_std " + setting.typeDecimal + "(8,3), " +
				"recall_avg " + setting.typeDecimal + "(8,3), " +
				"recall_std " + setting.typeDecimal + "(8,3), " +
				"auc_optimistic_avg " + setting.typeDecimal + "(8,3), " +
				"auc_optimistic_std " + setting.typeDecimal + "(8,3), " +
				"auc_average_avg " + setting.typeDecimal + "(8,3), " +
				"auc_average_std " + setting.typeDecimal + "(8,3), " +
				"auc_pessimistic_avg " + setting.typeDecimal + "(8,3), " +
				"auc_pessimistic_std " + setting.typeDecimal + "(8,3), " +
				"correlation_avg " + setting.typeDecimal + "(8,3), " +
				"correlation_std " + setting.typeDecimal + "(8,3), " +
				"rmse_avg " + setting.typeDecimal + "(8,3), " +                 // The precision can be too small...
				"rmse_std " + setting.typeDecimal + "(8,3), " +
				"relative_error_avg " + setting.typeDecimal + "(8,3), " +
				"relative_error_std " + setting.typeDecimal + "(8,3))";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalRun);

		return Network.executeUpdate(setting.dataSource, sql);
	}

	// Log the result of Predictor Factory run
	// ADD: column with count of produced predictors & whether PF finished successfully & setting.
	// Note: We are using preparedStatement because Oracle is sensitive on the format in which TimesTamp is passed.
	public static boolean addJournalRun(Setting setting, long elapsedTime) {

		// Log the time in a nice-to-read format
		logger.debug("Time of finishing: " + LocalDate.now() + " " + LocalTime.now());
		logger.debug("Run time: " + DurationFormatUtils.formatDurationWords(elapsedTime, true, true));

		// Tell us how greedy you are
		Memory.logMemoryInfo();

		// Tell us how buggy you are
		logger.debug("Info event count: " + CountAppender.getCount(Level.INFO));
		logger.debug("Warn event count: " + CountAppender.getCount(Level.WARN));
		logger.debug("Error event count: " + CountAppender.getCount(Level.ERROR));

		// Log the status into the database
		String sql = "insert into @outputTable " +
				"(schema_name, run_time, finish_time, memory, warn_count, error_count) " +
				"values (?, ?, ?, ?, ?, ?)";
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalRun);

		try (Connection connection = setting.dataSource.getConnection();
		     PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setString(1, setting.inputSchema);
			ps.setDouble(2, elapsedTime / 1000.0); // In seconds. Note: Do not use bigDecimal as it is unsupported on SAS
			ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
			ps.setDouble(4, Memory.usedMemory());   // In MB
			ps.setInt(5, CountAppender.getCount(Level.WARN));
			ps.setInt(6, CountAppender.getCount(Level.ERROR));
			ps.executeUpdate();
		} catch (SQLException ignored) {
			return false;
		}

		return true;
	}


	// 1a) Return create journal_predictor table command
	// Return true if the journal table was successfully created.
	// Note: Default values are not supported on SAS data sets -> avoid them.
	public static boolean getJournalPredictor(Setting setting) {
		logger.debug("# Setting up journal table #");

		// The primary key is set directly behind the column name, not at the end, because SAS supports only the first declaration.
		String sql = "CREATE TABLE @outputTable (" +
				"predictor_id " + setting.typeInteger + " PRIMARY KEY, " + // Let the database give the PK a unique name (that allows the user to make multiple copies of the journal)
				"group_id " + setting.typeInteger + ", " +
				"start_time " + setting.typeTimestamp + ", " +
				"run_time " + setting.typeDecimal + "(18,3), " +  // Old MySQL and SQL92 do not have/require support for fractions of a second.
				"predictor_name " + setting.typeVarchar + "(255), " + // In MySQL pure char is limited to 255 bytes -> stick to this value if possible
				"predictor_long_name " + setting.typeVarchar + "(512), " +
				"table_name " + setting.typeVarchar + "(1024), " +    // Table is a reserved keyword -> use table_name
				"column_list " + setting.typeVarchar + "(1024), " +
				"propagation_path " + setting.typeVarchar + "(1024), " +
				"propagation_depth " + setting.typeInteger + ", " +
				"date_constrain " + setting.typeVarchar + "(255), " +
				"parameter_list " + setting.typeVarchar + "(1024), " +
				"pattern_name " + setting.typeVarchar + "(255), " +
				"pattern_author " + setting.typeVarchar + "(255), " +
				"pattern_code " + setting.typeVarchar + "(3600), " +  // For example code for WoE is close to 1024 chars and NB is 3000
				"sql_code " + setting.typeVarchar + "(3600), " + // For example code for WoE is close to 1024 chars and NB is 3000
				"data_type " + setting.typeVarchar + "(255), " +
				"category_type " + setting.typeVarchar + "(255), " +
				"target " + setting.typeVarchar + "(255), " +
				getRelevanceDefinition(setting) +   // Chi2+conceptDrift for each target. Note: The variability of the name will cause troubles in external code (dashboard...). Can either use user friendly targetColumn identifiers or computer friendly baseTarget identifiers.
				"qc_row_count " + setting.typeInteger + ", " +
				"qc_null_count " + setting.typeInteger + ", " +
				"exception_message " + setting.typeVarchar + "(255), " +
				"is_ok " + setting.typeInteger + ", " +
				"is_duplicate " + setting.typeInteger + ", " +
				"duplicate_name " + setting.typeVarchar + "(255))";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPredictor);

		return Network.executeUpdate(setting.dataSource, sql);
	}

	private static String getRelevanceDefinition(Setting setting) {
		String result = "";

		for (String targetColumn : setting.targetColumnList) {
			result += "relevance_" + targetColumn + " " + setting.typeDecimal + "(18,3), ";  // Note: The string can be too long
			result += "concept_drift_" + targetColumn + " " + setting.typeDecimal + "(7,6), ";
			result += "weighted_relevance_" + targetColumn + " " + setting.typeDecimal + "(18,6), ";
		}

		return result;
	}

	// 1b) Add record into the journal_predictor
	// Return true if the journal table was successfully updated.
	// Note: should deal properly with storing nulls
	public static boolean addToJournalPredictor(Setting setting, Predictor predictor) {

		// Convert bool to int
		int isOk = predictor.isOk() ? 1 : 0;
		int isInferiorDuplicate = predictor.isInferiorDuplicate() ? 1 : 0;

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
				predictor.getRuntime() + ", " +
				"'" + predictor.getName() + "', " +
				"'" + predictor.getLongName() + "', " +
				"'" + predictor.getOriginalTable() + "', " +
				"'" + predictor.getColumnMap().toString() + "', " +      // Should be a list...
				"'" + predictor.getPropagationPath().toString() + "', " +
				predictor.getPropagationPath().size() + ", " +
				"'" + predictor.getPropagationDate() + "', " +
				"'" + predictor.getParameterMap().toString() + "', " +  // Violates the 1st norm...
				"'" + predictor.getPatternName() + "', " +
				"'" + predictor.getPatternAuthor() + "', " +
				"'" + predictor.getPatternCode().replaceAll("'", "''") + "', " +    // Escape single quotes
				"'" + predictor.getSql().replaceAll("'", "''") + "', " +        // Escape single quotes
				"'" + predictor.getDataTypeName() + "', " +
				"'" + predictor.getDataTypeCategory() + "', " +
				"'" + predictor.getTargetColumn() + "', " +
				getRelevanceValues(setting, predictor) +    // Chi2+conceptDrift for each target
				predictor.getRowCount() + ", " +
				predictor.getNullCount() + ", " +
				"'" + predictor.getExceptionMessage() + "', " +
				isOk + ", " +
				isInferiorDuplicate + ", " +
				"'" + predictor.getDuplicateName() + "')";

		return Network.executeUpdate(setting.dataSource, sql);
	}

	private static String getRelevanceValues(Setting setting, Predictor predictor) {
		String result = "";

		for (String baseTarget : setting.baseTargetList) {
			result += predictor.getRelevance(baseTarget) + ", ";            // Chi2
			result += predictor.getConceptDrift(baseTarget) + ", ";         // conceptDrift
			result += predictor.getWeightedRelevance(baseTarget) + ", ";    // Chi2*conceptDrift
		}

		return result;
	}

	public static boolean getJournalTable(Setting setting) {
		logger.debug("# Setting up journal table for propagated tables #");

		// An important limitation: Oracle limits name length of an identifier to 30 characters
		String sql = "CREATE TABLE @outputTable (" +
				"table_id " + setting.typeInteger + " PRIMARY KEY, " + // Let the database give the PK a unique name (that allows the user to make multiple copies of the journal)
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
		String temporalConstraint = table.temporalConstraint == null ? "NULL" : "'" + table.temporalConstraint + "'";


		// Assembly the head of the insert command
		String sql = "INSERT INTO @outputTable VALUES (";
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalTable);

		// Add the payload (which should not be transformed)
		sql += table.propagationOrder + ", " +
				timestampDesigned + ", " +
				table.timestampDesigned.until(table.timestampDelivered, ChronoUnit.MILLIS) / 1000.0 + ", " +
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
				"'" + table.sql.replaceAll("'", "''") + "', " +     // Escape single quotes
				isIdUnique + ", " +
				isUnique + ", " +
				isSuccessfullyExecuted + ", " +
				table.rowCount + ", " +
				isOk + ")";

		return Network.executeUpdate(setting.dataSource, sql);
	}


	public static boolean getJournalPattern(Setting setting) {
		String sql = "CREATE TABLE @outputTable (" +
				"name varchar(255) PRIMARY KEY, " +
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
				ps.setInt(3, "n".equals(predictor.getPatternCardinality()) ? 1 : 0);
				ps.setInt(4, columnSet.size() > 1 ? 1 : 0);
				ps.setInt(5, contains(predictor, "@nominalColumn"));
				ps.setInt(6, contains(predictor, "@numericalColumn"));
				ps.setInt(7, contains(predictor, "@temporalColumn"));
				ps.setInt(8, contains(predictor, "@baseTarget"));
				ps.setInt(9, contains(predictor, "@baseDate"));
				ps.setString(10, predictor.getPatternDescription().replaceAll(" +", " ").replaceAll("\\t", "").trim());
				ps.executeUpdate();
			}
		} catch (SQLException ignored) {
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
		String sql = "CREATE TABLE @outputTable (" +
				"temporal_constraint_id " + setting.typeInteger + " PRIMARY KEY, " + // Let the database give the PK a unique name (that allows the user to make multiple copies of the journal)
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
		String target = "";
		String dateAs = "";
		String dateAsTable = "";
		String dateCondition = "";

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Detect duplicates in the base table
		boolean isTargetTupleUnique = setting.dialect.isTargetTupleUnique(setting, setting.targetTable);

		// Use date?
		if (setting.targetDate != null) {
			dateAs = " @targetDate AS " + escapeAlias(setting, setting.baseDate) + ", ";
			dateAsTable = " t1.@targetDate AS " + escapeAlias(setting, setting.baseDate) + ",";
			dateCondition = " WHERE @targetDate IS NOT NULL";
		}

		// Deduplicate the base table if necessary
		if (isTargetTupleUnique) {
			// Prepare aliases for targetId
			for (int i = 0; i < setting.baseIdList.size(); i++) {
				id = id + QL + setting.targetIdList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseIdList.get(i)) + ", ";
			}

			// Prepare aliases for targetColumn
			for (int i = 0; i < setting.baseTargetList.size(); i++) {
				target = target + QL + setting.targetColumnList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseTargetList.get(i)) + ",";
			}

			// The query itself
			sql = "SELECT " + id + dateAs + target + " FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " FROM @targetTable" + dateCondition;
		} else {
			logger.warn("The base table contains duplicate values in {BaseID, BaseDate}. " +
					"Continuing without ALL duplicate values. " +
					"The results will be incomplete and possibly biased. " +
					"To get correct results create an artificial key / use time column with higher precision...");

			// Prepare aliases for targetId
			for (int i = 0; i < setting.baseIdList.size(); i++) {
				id = id + " t1." + QL + setting.targetIdList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseIdList.get(i)) + ",";
			}

			// Prepare aliases for targetColumns
			for (int i = 0; i < setting.baseTargetList.size(); i++) {
				target = target + " t1." + QL + setting.targetColumnList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseTargetList.get(i)) + ",";
			}

			// The query itself (two scenarios to avoid putting everything like a puzzle)
			if (setting.targetDate == null) {
				sql = "SELECT" + id + target + " FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " " +
						"FROM @targetTable t1 LEFT JOIN (" +
						"SELECT @targetId FROM @targetTable GROUP BY @targetId HAVING count(*)>1 " +
						") t2 " +
						"ON t1.@targetId = t2.@targetId " +
						"WHERE t2.@targetId is null";
			} else {
				sql = "SELECT" + id + dateAsTable + target + " FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " " +
						"FROM @targetTable t1 LEFT JOIN (" +
						"SELECT @targetId, @targetDate FROM @targetTable GROUP BY @targetId, @targetDate HAVING count(*)>1 " +
						") t2 " +
						"ON t1.@targetId = t2.@targetId AND t1.@targetDate = t2.@targetDate " +
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
	// NOTE: CURRENTLY STRATIFIED JUST BY THE THE FIRST TARGET COLUMN
	public static void getSubSampleClassification(Setting setting, SortedMap<String, Table> metaInput) {

		// Initialization
		String sql = "";
		String targetColumn = setting.targetColumnList.get(0);
		Set<String> targetValueList = setting.targetUniqueValueMap.get(targetColumn);
		String baseTarget = setting.baseTargetList.get(0);
		String quote = "";

		// Iff the target is a string, quote the values with single quotes.
		String targetDataTypeName = metaInput.get(setting.targetTable).getColumn(targetColumn).dataTypeName.toUpperCase();
		if (targetDataTypeName.contains("CHAR") || targetDataTypeName.contains("TEXT")) {
			quote = "'";
		}

		// Create union
		// NOTE: We should rather use sample count divided by the count of non-null unique values in the target.
		// If some class is rare, the unused slots should be distributed uniformly into the rest of the classes.
		for (String targetValue : targetValueList) {
			sql = sql + "(" + Parser.limitResultSet(setting, "SELECT * FROM @baseTable WHERE " + escapeEntity(setting, baseTarget) + " = " + quote + targetValue + quote + "\n", setting.sampleCount) + ")";
			sql = sql + " UNION ALL \n";    // Add "union all" between all the selects.
		}

		// Finally, add unclassified records.
		sql = sql + "(" + Parser.limitResultSet(setting, "SELECT * FROM @baseTable WHERE " + escapeEntity(setting, baseTarget) + " is null\n", setting.sampleCount) + ")";

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
	public static String propagateID(Setting setting, MetaOutput.OutputTable table) {
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
		if (table.temporalConstraint != null) {
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
	public static String getPredictor(Setting setting, Predictor predictor) {
		String sql = predictor.getSql();

		sql = Parser.expandBase(setting, sql);
		sql = Parser.expandBasePartitionBy(setting, sql);

		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, predictor.getOutputTable());
		sql = escapeEntityPredictor(setting, sql, predictor);

		if ("SAS".equals(setting.databaseVendor)) {
			sql = sql.replace("\"", "");                // VERY UGLY WORKAROUND. SHOULD IMPLEMENT quoteAlias
		}

		return sql;
	}

	// 5) Assembly the final step - the output table
	public static void getAllMainSamples(Setting setting, Journal journal) {
		for (int i = 0; i < setting.baseTargetList.size(); i++) {
			String baseTarget = setting.baseTargetList.get(i);
			String targetColumn = setting.targetColumnList.get(i);
			Collection<Predictor> predictorList = journal.getTopPredictors(baseTarget);
			getMainSample(setting, predictorList, baseTarget, targetColumn);
			logger.info("#### Produced " + setting.outputSchema + "." + setting.mainTablePrefix + " with " + predictorList.size() + " most predictive predictors from " + journal.getEvaluationCount() + " evaluated. Duplicate or unsuccessfully calculated predictors are not passed into the output table. ####");
		}
	}

	// Create MainSample for a single target
	// Note: The current implementation stores only up to ~3600 predictors. So far the limit is acceptable as column
	// count in a table is commonly limited (1600 columns in PostgreSQL and 1000 columns in Oracle).
	// UNSYSTEMATIC ESCAPING
	private static void getMainSample(Setting setting, Collection<Predictor> predictorList, String baseTarget, String targetColumn) {

		// Extract table and column names.
		// ASSUMING THAT THE MATCH IS 1:1! (i.e. one predictor equals exactly one column)
		List<String> tableListAll = new ArrayList<>();
		List<String> columnListAll = new ArrayList<>();

		for (Predictor predictor : predictorList) {
			tableListAll.add(predictor.getOutputTable());
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

		// Target
		String target = "t1." + QL + baseTarget + QR;

		// Create temporary tables
		for (int i = 0; i < tableListSmall.size(); i++) {
			// Initialization
			StringBuilder stringBuffer = new StringBuilder(500);
			int tableCount;                                                 // The tables are named t1..t* in the join
			String tempTable = setting.mainTablePrefix + "_temp" + (100 + i);   // The name of the temporary table
			tempTableList.add(tempTable);
			List<String> tableList = tableListSmall.get(i);
			List<String> columnList = columnListSmall.get(i);

			// Select part
			stringBuffer.append("SELECT " + idList + date + target);
			tableCount = 2;
			for (String column : columnList) {
				stringBuffer.append(", t" + tableCount + ".@" + column);   // The column name will be escaped
				tableCount++;
			}

			// From part
			stringBuffer.append(" FROM @baseSampled t1");
			tableCount = 2;
			for (String table : tableList) {

				// Join condition
				String joinCondition = "";
				for (String id : setting.baseIdList) {
					joinCondition += "t1." + QL + id + QR + " = t" + tableCount + "." + QL + id + QR + " AND ";
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
			for (String table : tableList)
				map.put(table, table);           // This is dangerous (no prefix... in the substitution)
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
		stringBuffer.append("SELECT " + idList + date + target);

		for (int i = 0; i < tempTableList.size(); i++) {
			for (String column : columnListSmall.get(i)) {
				stringBuffer.append(", t" + (i + 2) + ".@" + column);    // The column name will be escaped
			}
		}

		// From part
		stringBuffer.append(" FROM @baseSampled t1");

		for (int i = 0; i < tempTableList.size(); i++) {
			int tableCount = i + 2;
			String tempTable = tempTableList.get(i);

			// Join condition
			String joinCondition = "";
			for (String id : setting.baseIdList) {
				joinCondition += "t1." + QL + id + QR + " = t" + tableCount + "." + QL + id + QR + " AND ";
			}
			joinCondition = StringUtils.stripEnd(joinCondition, " AND ");

			stringBuffer.append(" INNER JOIN " + tempTable + " t" + tableCount + " ON " + joinCondition);

			if (setting.targetDate != null) {
				stringBuffer.append(" AND t1.@baseDate = t" + tableCount + ".@baseDate");
			}
		}

		// Make SQL from the pattern
		String mainTable = setting.mainTablePrefix + "_" + targetColumn; // NOTE: The table name can be too long...
		String pattern_code = stringBuffer.toString();
		pattern_code = addCreateTableAs(setting, pattern_code);
		pattern_code = expandName(pattern_code);
		pattern_code = expandNameList(pattern_code, tempTableList);
		pattern_code = escapeEntity(setting, pattern_code, mainTable);

		// Escape table & column entities (tables can't be escaped in definition because they have to be first expanded...)
		Map<String, String> map = new HashMap<>(61);
		for (String table : tempTableList) map.put(table, table);
		for (String column : columnListAll) map.put("@" + column, column);
		String sql = escapeEntityMap(setting, pattern_code, map);

		// Execute the query
		Network.executeUpdate(setting.dataSource, sql);

		//// Clean up of temporary tables /////
		for (String table : tempTableList) {
			dropTable(setting, table);
		}

		//// Perform output Quality Control ////
		List<String> suspiciousPatternList = qcPredictors(setting);
		if (suspiciousPatternList.size() > 0) {
			logger.warn("Following patterns always failed: " + suspiciousPatternList.toString());
		}

		int columnCount = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainTable).size();
		if (columnCount < 3) {
			logger.warn("Table " + mainTable + " contains: " + columnCount + " columns"); // We expect at least: {targetId, targetColumn, 1 predictor}
		} else {
			logger.debug("Table " + mainTable + " contains: " + columnCount + " columns");
		}
	}

	// Subroutine - transform java date to SQL date
	private static String date2query(Setting setting, LocalDateTime date) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

		String template = setting.insertTimestampSyntax;
		String timestamp = date.format(formatter);
		timestamp = template.replace("@timestamp", timestamp);

		return timestamp;
	}

}

