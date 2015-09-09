package connection;

import com.google.common.collect.Lists;
import featureExtraction.Predictor;
import metaInformation.MetaOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import parser.ANTLR;
import run.Setting;
import utility.Meta;
import utility.Meta.Table;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

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
	protected static String addCreateViewAs(Setting setting, String sql) {

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
	    sql = sql.replace("@columnName",  predictor.getName());
	    
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
		sql = sql.replace("@outputTable", QL + table.propagatedName + QR);
		sql = sql.replace("@dateColumn", QL + table.constrainDate + QR);

		return sql;
	}

	// Subroutine: Get escaped alias. Oracle is using double quotes. MySQL single quotes...
	private static String escapeAlias(Setting setting, String alias) {
		return setting.quoteAliasOpen + alias + setting.quoteAliasClose;
	}
	
	// Subroutine: Is the predictor a string or a number? Just ask the database.
	// Note: The data type could be predicted from the pattern and pattern parameters. But the implemented 
	// method is foolproof, though slow.
	private static String getPredictorType(Setting setting, String table, String column) {
				
		SortedMap<String, Integer> allColumns = Meta.collectColumns(setting, setting.database, setting.outputSchema, table);
		SortedMap<String, Integer> columnMap = new TreeMap<>();
		if (allColumns.containsKey(column)) { // Take care of scenario, when there isn't the column
			columnMap.put(column, allColumns.get(column));
		}
		Table tableStruct = new Table(); 
		tableStruct = Meta.categorizeColumns(tableStruct, columnMap, table);
		
		if (tableStruct.nominalColumn.contains(column)) {
			return "nominal";
		} else if (tableStruct.timeColumn.contains(column)) {
			return "time";
		} 
			
		return "numerical"; 
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
		
		return Network.executeUpdate(setting.connection, sql);
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

		return Network.executeUpdate(setting.connection, sql);
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
		SortedSet<String> tableSet = utility.Meta.collectTables(setting, setting.database, setting.outputSchema);
		SortedMap<String, String> dropMap = new TreeMap<>();
		
		// Select tables for dropping
		for (String table : tableSet) {
			if (table.startsWith(setting.mainTable)) dropMap.put(1 + table, table);			// Mainsample and it's temporary tables
			if (table.startsWith(setting.predictorPrefix)) dropMap.put(2 + table, table);	// Predictors
			if (table.startsWith(setting.propagatedPrefix)) dropMap.put(3 + table, table);	// Propagated tables
			if (table.equals(setting.baseSampled)) dropMap.put(4 + table, table);			// Sampled base table
			if (table.equals(setting.journalTable)) dropMap.put(5 + table, table);			// Journal table
			if (table.equals(setting.journalPropagationTable)) dropMap.put(6 + table, table);	// Journal propagated table
		}

		// Drop the tables
		for (String table : dropMap.values()) {
			dropTable(setting, table);
		}

		// Drop the view
		dropView(setting, setting.baseTable);				// Base table
	}
	
	// Create index on {baseId, baseDate}.
	// Returns true if the update was successful.
	public static boolean addIndex(Setting setting, String outputTable) {
		String columns = "(@baseId)";
		if (setting.targetDate != null) {
			columns = "(@baseId, @baseDate)";
		}

		// We should be sure that the index name is not too long
		String name = outputTable + "_idx";
		if (outputTable.startsWith(setting.propagatedPrefix)) {
			name = outputTable.substring(setting.propagatedPrefix.length(), outputTable.length()) + "_idx";
		} else if (outputTable.startsWith(setting.mainTable)) {
			name = outputTable.substring(setting.mainTable.length(), outputTable.length()) + "_idx";
		}

		String sql = "CREATE INDEX " + name + " ON @outputTable " + columns;

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);
		
		boolean isOK = Network.executeUpdate(setting.connection, sql);
		
		return isOK;
	}
	
	// Set primary key for a table in the output schema.
	public static boolean setPrimaryKey(Setting setting, String outputTable) {
		String columns = "(@baseId)";
		if (setting.targetDate != null) {
			columns = "(@baseId, @baseDate)";
		}

		String sql = "ALTER TABLE @outputTable ADD PRIMARY KEY " + columns;

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);

		boolean isOK = Network.executeUpdate(setting.connection, sql);

		return isOK;
	}
	
	
	// Get rowCount for a table in the output schema
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
		
		List<String> resultList = Network.executeQuery(setting.connection, sql);
		
		// If the table doesn't exist, the resultSet is empty. Return 0.
		if (resultList.isEmpty()) return 0;
		
		// Otherwise return the actual row count
		return (int)Double.parseDouble(resultList.get(0)); // SAS can return 682.0. SHOULD IMPLEMENT LIST<INTEGERS>.
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
		
		List<String> resultList = Network.executeQuery(setting.connection, sql);
		
		// If the table doesn't exist, the resultSet is empty. Return 0.
		if (resultList.isEmpty()) return 0;
		
		return (int)Double.parseDouble(resultList.get(0)); // SAS can return 682.0
	}
	
	// Get the maximal cardinality of the table in respect to targetId. If the cardinality is 1:1, 
	// we may want to remove the bottom time constrain in base propagation.
	// The map should contain @inputTable and @targetId2.
	// The indexes start at two because it was convenient to reuse the parameters in Propagation function.
	// Note that we are working with the input tables -> alter commands are forbidden.
	// It was difficult to write a version that would be more effective than sum(... having count(*)>1).
	// If you decide to replace this query, test it with several database engines!
	// IS NOT USING SYSTEM ESCAPING
	public static boolean isIdUnique(Setting setting, MetaOutput.OutputTable table) {
		String sql = "SELECT count(*) " +
					 "FROM @inputTable " +
					 "GROUP BY @idCommaSeparated " +
					 "HAVING COUNT(*)>1";

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
		
		boolean result = Network.isResultSetEmpty(setting.connection, sql);
		
		if (result) logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " doesn't contain duplicates #");
		else logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " CONTAINS duplicates #");
		
		return result;
	}

	// Check whether the columns {baseId, baseDate} are unique in the table.
	// If the columns are unique, we may avoid of aggregation and copy the values immediately. 
	// Note that in comparison to getIdCardinality we can modify the tables as we are working with outputSchema. 
	public static boolean isUnique(Setting setting, String table, boolean useInputSchema) {
		// We could have used possibly faster: "ALTER TABLE @outputTable ADD UNIQUE (@baseId, @baseDate)".
		// But it would not work in Netezza as Netezza doesn't support constraint checking and referential integrity.
		// I also tested "SELECT (CASE WHEN EXISTS(SELECT 1 FROM @outputTable GROUP BY @baseId, @baseDate HAVING COUNT(*)>1) THEN 1 ELSE 0 END)"
		// But it doesn't work in Oracle as Oracle requires "from dual" -> use the simplest possible query
		// and retrieve only one row with JDBC.
		String sql;

		if (useInputSchema) {
			sql = "SELECT count(*) " +
					 "FROM @targetTable " +
					 "GROUP BY @targetId" + (setting.targetDate==null ? " " : ", @targetDate ") +
					 "HAVING COUNT(*)>1";
		} else {
			sql = "SELECT count(*) " +
					 "FROM @outputTable " +
					 "GROUP BY @baseId" + (setting.targetDate==null ? " " : ", @baseDate ") +
					 "HAVING COUNT(*)>1";
		}
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);
		
		boolean result = Network.isResultSetEmpty(setting.connection, sql);
		return result;	// If the result set is empty, all values are unique. 
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
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("@columnName", columnName);
		map.put("@inputTable", tableName); // To cover the scenario that it's in the input schema 
		sql = escapeEntityMap(setting, sql, map);
		
		return Network.executeQuery(setting.connection, sql);
	}
	
	// Could the two columns in the table describe a symmetric relation (like in borderLength(c1, c2))?
	// DEVELOPMENTAL AND LIKELY USELESS...
	public static boolean isSymmetric(Setting setting, HashMap<String, String> map) {
		String sql = "SELECT CASE "
					+ "         WHEN EXISTS (SELECT @lagColumn, "
					+ "                             @column "
					+ "                      FROM @inputTable"
					+ "                      EXCEPT "
					+ "                      SELECT @column, "
					+ "                             @lagColumn "
					+ "                      FROM @inputTable) THEN 'no' "
					+ "         ELSE 'yes' "
					+ "         END AS symmetric";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityMap(setting, sql, map);

		List<String> result = Network.executeQuery(setting.connection, sql);
		
		return "yes".equals(result.get(0));
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
 			sql = "select count(*)*power(corr(t2.average, t1.@baseTarget), 2) " +
					"from @outputTable t1 " +
					"join ( " +
						"select @column " + 
							", cast(avg(@baseTarget) as decimal(38, 10)) as average " +	// NOT NICE: Solves frequent arithmetic errors on MSSQL.
						"from @outputTable " +
						"group by @column " +
					") t2 " +
					"on t1.@column = t2.@column" +
					"where t1.@column is not null and t1.@baseTarget is not null";
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
		HashMap<String, String> fieldMap = new HashMap<String, String>();
		fieldMap.put("@column", column);
		fieldMap.put("@baseTarget", setting.baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);
		
		// Execute the SQL
		List<String> response = Network.executeQuery(setting.connection, sql);
		
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
	// SHOULD BE EXTENDED TO SUPPORT BOOLEANS
 	public static double getChi2(Setting setting, String table, String column) {
 		// Initialization
 		String sql;
 		String predictorType = getPredictorType(setting, table, column);
 		
 		// Technical: MySQL or PostgreSQL require correlation name (also known as alias) for derived tables. 
 		// But Oracle does not accept "as" keyword in front of the table alias. See:
 		// http://www.techonthenet.com/oracle/alias.php
 		// Hence use following syntax: (select col1 from table t1) t2. 
 		if ("nominal".equals(predictorType)) {
 			// Use categorical column directly
 			sql = "select sum(chi2)/count(distinct(bin)) " // Linearly regularized against columns with high cardinality
				+ "from ( "
				+ "	select (expected.expected-measured.count) * (expected.expected-measured.count) / expected.expected AS chi2 "
				+ " expected.bin AS bin"
				+ "	from ( "
				+ "		select expected_bin.count*expected_target.prob AS expected "
				+ "			 , bin "
				+ "			 , target "
				+ "		from ( "
				+ "			select @baseTarget target "
				+ "				 , cast(count(*) as DECIMAL)/max(t2.nrow) AS prob "
				+ "			from @outputTable, ( "
				+ "				select cast(count(*) as DECIMAL) AS nrow "
				+ "				from @outputTable "
				+ "			) t2 "
				+ "			GROUP BY @baseTarget "
				+ "		) expected_target, ( "
				+ "			select cast(count(*) as DECIMAL) AS count "
				+ "				 , @column AS bin "
				+ "			from @outputTable "
				+ "			group by @column "
				+ "		) expected_bin "
				+ "	) expected "
				+ "	left join ( "
				+ "		select @baseTarget AS target "
				+ "			 , cast(count(*) as DECIMAL) AS count "
				+ "			 , @column AS bin "
				+ "		from @outputTable "
				+ "		group by @column, @baseTarget "
				+ "	) measured "
				+ "	on expected.bin = measured.bin "
				+ "	and expected.target = measured.target "
				+ ") chi2";
 		} else {
 			// Group numerical/time values into 10 bins.
 			// If desirable you can optimize the optimal amount of bins with Sturge's rule 
 			// but syntax for log is different in each database. 
 			sql = "select sum(chi2)/10 " // To match regularization of nominal columns
				+ "from ( "
				+ "	select (expected.expected-measured.count) * (expected.expected-measured.count) / expected.expected AS chi2 "
				+ "	from ( "
				+ "		select expected_bin.count*expected_target.prob AS expected "
				+ "			 , bin "
				+ "			 , target "
				+ "		from ( "
				+ "			select @baseTarget AS target "
				+ "				 , cast(count(*) as DECIMAL)/max(t2.nrow) AS prob "
				+ "			from @outputTable, ( "
				+ "				select cast(count(*) as DECIMAL) AS nrow "
				+ "				from @outputTable "
				+ "			) t2 "
				+ "			GROUP BY @baseTarget "
				+ "		) expected_target, ( "
				+ "			select cast(count(*) as DECIMAL) AS count "
				+ "				 , floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)) AS bin " // Bin really into 10 bins.
				+ "			from @outputTable, ( "
				+ "					select (max(@column)-min(@column)) / 10 AS bin_width "
				+ "						 , min(@column) AS min_value "
				+ "					from @outputTable "
				+ "				) t2 "
				+ "			group by floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)) "	// And avoid division by zero.
				+ "		) expected_bin "
				+ "	) expected "
				+ "	left join ( "
				+ "		select @baseTarget target "
				+ "			 , cast(count(*) as DECIMAL) AS count "
				+ "			 , floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)) AS bin "
				+ "		from @outputTable, ( "
				+ "				select (max(@column)-min(@column)) / 10 AS bin_width "
				+ "					 , min(@column) AS min_value "
				+ "				from @outputTable "
				+ "			) t2 "
				+ "		group by floor((@column-t2.min_value) / (t2.bin_width + 0.0000001)), @baseTarget "
				+ "	) measured "
				+ "	on expected.bin = measured.bin "
				+ "	and expected.target = measured.target "
				+ ") chi2";
 			
 			// For time columns just cast time to number.
 			if ("time".equals(predictorType)) {
 				sql = sql.replace("@column", setting.dateToNumber);
 			}
 		} 

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);	/* outputTable, outputSchema, output.Database */
		
		// Escape the entities
		HashMap<String, String> fieldMap = new HashMap<String, String>();
		fieldMap.put("@column", column);
		fieldMap.put("@baseTarget", setting.baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);

		// Execute the SQL
		List<String> response = Network.executeQuery(setting.connection, sql);
		
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
 		String sql = "select pattern_name " +
					 "from @outputTable " +
					 "group by pattern_name " +
					 "having avg(is_ok) = 0";	
 		
 		sql = expandName(sql);
 		sql = escapeEntity(setting, sql, "journal");
 		List<String> badPatterns = Network.executeQuery(setting.connection, sql);
 		
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
 		String sql = "create table @outputSchema.@ms as select * from @outputTable";
 		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "mainSample");
		HashMap<String, String> fieldMap = new HashMap<String, String>();
		fieldMap.put("@ms", "ms_" + setting.inputSchema);
		sql = escapeEntityMap(setting, sql, fieldMap);
		Network.executeUpdate(setting.connection, sql);
 		
		// Drop the previous version
 		dropTable(setting, "j_" + setting.inputSchema);
 		
 		// Copy the current journal
 		sql = "create table @outputSchema.@j as select * from @outputTable";
 		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "journal");
		fieldMap = new HashMap<String, String>();
		fieldMap.put("@j", "j_" + setting.inputSchema);
		sql = escapeEntityMap(setting, sql, fieldMap);
		Network.executeUpdate(setting.connection, sql);
		
		// Log time
		sql = "insert into @outputTable (schema_name, runtime) values ('" + setting.inputSchema + "', " + elapsedTime + ")";
 		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "log");
		Network.executeUpdate(setting.connection, sql);
 	}
 	

	// 1a) Return create journal_predictor table command
	// Return true if the journal table was successfully created.
	// Note: Default values are not supported on SAS data sets -> avoid them.
	public static boolean getJournal(Setting setting) {
		logger.debug("# Setting up journal table #");
		
		String sql = "CREATE TABLE @outputTable ("+
	      "predictor_id " + setting.typeInteger + ", " +
	      "group_id " + setting.typeInteger + ", " +
	      "start_time " + setting.typeTimestamp + ", " +
	      "run_time " + setting.typeDecimal + "(18,3), " +  // Old MySQL and SQL92 do not have/require support for fractions of a second. 
	      "predictor_name " + setting.typeVarchar + "(255), " +	// In MySQL pure char is limited to 255 bytes -> stick to this value if possible
	      "table_name " + setting.typeVarchar + "(1024), " +	// Table is a reserved keyword -> use table_name
	      "column_list " + setting.typeVarchar + "(1024), " +
	      "propagation_path " + setting.typeVarchar + "(1024), " +
	      "propagation_depth " + setting.typeInteger + ", " +	
	      "date_constrain " + setting.typeVarchar + "(255), " +
	      "parameter_list " + setting.typeVarchar + "(1024), " +
	      "pattern_name " + setting.typeVarchar + "(255), " + 
	      "pattern_author " + setting.typeVarchar + "(255), " + 
	      "pattern_code " + setting.typeVarchar + "(2024), " +	// For example code for WoE is close to 1024 chars
	      "sql_code " + setting.typeVarchar + "(2024), " + // For example code for WoE is close to 1024 chars
	      "target " + setting.typeVarchar + "(255), " +
	      "relevance " + setting.typeDecimal + "(18,3), " +
	      "qc_rowCount " + setting.typeInteger + ", " +
	      "qc_nullCount " + setting.typeInteger + ", " +
	      "is_ok " + setting.typeInteger + ", " +
	      "CONSTRAINT pk_journal PRIMARY KEY (predictor_id))";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalTable);
		
		return Network.executeUpdate(setting.connection, sql);
	}
	 
	// 1b) Add record into the journal_predictor
	// Return true if the journal table was successfully updated.
	public static boolean addToJournal(Setting setting, Predictor predictor) {

		// Convert bool to int
		int isOk = predictor.isOk()? 1 : 0;
		
		// Insert timestamp subquery
		String timestampBuild = date2query(setting, predictor.getTimestampBuilt());
		
		// Assembly the insert
		String sql = "INSERT INTO @outputTable VALUES (" +
	        predictor.getId() + ", " +
	        predictor.getGroupId() + ", " +
	              timestampBuild + ", " +
	              predictor.getTimestampBuilt().until(predictor.getTimestampDelivered(), ChronoUnit.MILLIS)/1000.0 + ", " +
	        "'" + predictor.getName() + "', " +
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
	              isOk + ")";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalTable);

		return Network.executeUpdate(setting.connection, sql);
	}

	public static boolean getJournalPropagation(Setting setting) {
		logger.debug("# Setting up journal table for propagated tables #");

		String sql = "CREATE TABLE @outputTable ("+
				"table_id " + setting.typeInteger + ", " +
				"start_time " + setting.typeTimestamp + ", " +
				"run_time " + setting.typeDecimal + "(18,3), " +
				"table_name " + setting.typeVarchar + "(255), " +
				"original_name " + setting.typeVarchar + "(255), " +
				"date_constrain " + setting.typeVarchar + "(255), " +
				"candidate_date_list " + setting.typeVarchar + "(2024), " +
				"candidate_date_count " + setting.typeInteger + ", " +
				"propagation_path " + setting.typeVarchar + "(1024), " +
				"propagation_depth " + setting.typeInteger + ", " +
				"join_on_list " + setting.typeVarchar + "(1024), " +
				"sql_code " + setting.typeVarchar + "(2024), " + // For example code for WoE is close to 1024 chars
				"is_id_unique " + setting.typeInteger + ", " +
				"is_unique " + setting.typeInteger + ", " +
				"qc_rowCount " + setting.typeInteger + ", " +
				"qc_successfullyExecuted " + setting.typeInteger + ", " +
				"CONSTRAINT pk_journal_propagated PRIMARY KEY (table_id))";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPropagationTable);

		return Network.executeUpdate(setting.connection, sql);
	}

	public static boolean addToJournalPropagation(Setting setting, MetaOutput.OutputTable table) {

		// Convert bool to int
		int isOk = table.isSuccessfullyExecuted ? 1 : 0;
		int isIdUnique = table.isIdUnique ? 1 : 0;
		int isUnique = table.isUnique ? 1 : 0;

		// Convert date to query
		String timestampDesigned = date2query(setting, table.timestampDesigned);

		// Assembly the insert
		String sql = "INSERT INTO @outputTable VALUES (" +
				table.propagationOrder + ", " +
				timestampDesigned + ", " +
				table.timestampDesigned.until(table.timestampDelivered, ChronoUnit.MILLIS)/1000.0 + ", " +
				"'" + table.propagatedName + "', " +
				"'" + table.originalName + "', " +
				"'" + table.constrainDate + "', " +
				"'" + table.timeColumn + "', " +
				table.timeColumn.size() + ", " +
				"'" + table.propagationPath.toString() + "', " +
				table.propagationPath.size() + ", " +
				"'" + table.propagationForeignConstraint.fColumn + "', " +
				"'" + table.sql.replaceAll("'", "''") + "', " +		// Escape single quotes
				isIdUnique + ", " +
				isUnique + ", " +
				table.rowCount + ", " +
				isOk + ")";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalPropagationTable);

		return Network.executeUpdate(setting.connection, sql);
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
		String dateAndCondition = "";

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Detect duplicates in the base table
		boolean isUnique = isUnique(setting, setting.targetTable, true);

		// Use date?
		if (setting.targetDate != null) {
			dateAs = " @targetDate AS " + escapeAlias(setting, setting.baseDate) + ",";
			dateAsTable =  " t1.@targetDate AS " + escapeAlias(setting, setting.baseDate) + ",";
			dateCondition = " WHERE @targetDate IS NOT NULL";
			dateAndCondition = " AND t1.@targetDate is not null";
		}

		// Deduplicate the base table if necessary
		if (!isUnique) {
			logger.warn("The base table contains duplicate values in {BaseID, BaseDate}. " +
					"Continuing without ALL duplicate values. " +
					"The results will be incomplete and possibly biased. " +
					"To get correct results create an artificial key / use time column with higher precision...");

			// Prepare aliases for targetId
			for (int i = 0; i < setting.baseIdList.size(); i++) {
				id = id + " t1." + QL + setting.targetIdList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseIdList.get(i)) + ",";
			}

			// The query itself
			sql = "SELECT" + id + dateAsTable + " t1.@targetColumn AS " + escapeAlias(setting, setting.baseTarget) + ", FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " " +
					"FROM @targetTable t1 LEFT JOIN (" +
					"SELECT @targetId FROM @targetTable GROUP BY @targetId" + " HAVING count(*)>1 " +
					") t2 " +
					"ON t1.@targetId = t2.@targetId " +
					"WHERE t2.@targetId is null" + dateAndCondition;
		} else {
			// Prepare aliases for targetId
			for (int i = 0; i < setting.baseIdList.size(); i++) {
				id = id + QL + setting.targetIdList.get(i) + QR + " AS " + escapeAlias(setting, setting.baseIdList.get(i)) + ", ";
			}

			// The query itself
			sql = "SELECT " + id + dateAs + " @targetColumn AS " + escapeAlias(setting, setting.baseTarget) + ", FLOOR(" + setting.randomCommand + " * 10) AS " + escapeAlias(setting, setting.baseFold) + " FROM @targetTable" + dateCondition;
		}
		
		// Assembly the query
		sql = addCreateViewAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.baseTable);
		
		// Execute the query
		boolean isCreated = Network.executeUpdate(setting.connection, sql);
		
		if (!isCreated) {
			logger.warn("The base table was not successfully created.");
		}

		return (isUnique && isCreated);
	}
	

	// Subsample base table based on target class.
	// Note: The selection is not guaranteed to be random.
	public static void getSubSampleClassification(Setting setting, SortedMap<String, Table> metaInput) {
		
		// Initialization
		String sql = "";
		List<String> targetValueList = metaInput.get(setting.targetTable).uniqueList.get(setting.targetColumn);
		String quote = "";
		
		// Iff the target is nominal, quote the values with single quotes.
		if (setting.isTargetNominal) {
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
		
		Network.executeUpdate(setting.connection, sql);

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

		Network.executeUpdate(setting.connection, sql);

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

		// Escape the select part
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
		if (table.constrainDate!=null) {
			// First the upper bound (lead)
			sql = sql + " WHERE t2.@dateColumn <= " + setting.dateAddSyntax;
			sql = sql.replaceAll("@amount", "-" + setting.lead.toString()); // Negative lead		
			
			// Then, if required, add the lower bound (lag)
			if (table.dateBottomBounded) {
				sql = sql + " AND " + setting.dateAddSyntax + " <= t2.@dateColumn";
				Integer leadLag = setting.lead + setting.lag;
				sql = sql.replaceAll("@amount", "-" + leadLag.toString()); // Negative lead+lag
			}

			sql = sql.replaceAll("@datePart", setting.unit);
		}
						
		// Pattern_code to SQL conversion
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table.propagatedName);
		sql = escapeEntityTable(setting, sql, table);
					
		return sql;
	}
	
	// 4) Get predictor
	public static String getPredictor(Setting setting, Predictor predictor){
		String sql = predictor.getSql();

		sql = Parser.expandBase(setting, sql);

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
	public static void getMainSample(Setting setting, List<Predictor> journal) {
		
		// Consider only good predictors 
		List<Predictor> predictorList = journal.stream().filter(p -> p.isOk()).collect(Collectors.toList());
				
		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.RelevanceComparator.reversed());
		
		// Keep top N predictors per groupId
		Collections.sort(predictorList, Predictor.GroupIdComparator);
		
		int lagGroupId = 0;
		List<Predictor> predictorList2 = new ArrayList<>();
		
		for (Predictor predictor : predictorList) {
			if (predictor.getGroupId() != lagGroupId) {	// For each groupId select top n predictors
				predictorList2.addAll(getTopN(predictorList, predictor));
			}
			lagGroupId = predictor.getGroupId();
		}

		// Once again, resort the relevances in descending order
		Collections.sort(predictorList2, Predictor.RelevanceComparator.reversed());

		// Cap the amount of predictors by columnMax
		// RESERVE 3 COLUMNS FOR ID, TARGET AND TIME
		// THE COUNT OF COLUMNS IS ALSO LIMITED BY ROW SIZE. AND BIGINT COMPOSES OF 8 BYTES -> DIVIDE BY 8.
		// BUT THIS IS JUST A ROUGH ESTIMATE - ID CAN BE COMPOSED OF MULTIPLE COLUMNS...
		predictorList = predictorList2.subList(0, Math.min(setting.columnMax/8-3, predictorList2.size()));
		
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
			Network.executeUpdate(setting.connection, sql);
			
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
		Network.executeUpdate(setting.connection, sql);
		
		
		//// Perform output Quality Control ////
		List<String> suspiciousPatternList = qcPredictors(setting);
		if (suspiciousPatternList.size()>0) {
			logger.warn("Following patterns always failed: " + suspiciousPatternList.toString());
		}
		
		int columnCount = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable).size();
		logger.debug("MainSample table contains: " + columnCount + " columns (the limit is: " + (setting.columnMax/8) + ")");
	}

	private static String date2query(Setting setting, LocalDateTime date) {
		DateTimeFormatter formatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

		String template = setting.insertTimestampSyntax;
		String timestamp = date.format(formatter);
		timestamp = template.replace("@timestamp", timestamp);

		return timestamp;
	}

	// Subroutine - return top n predictors from the list. Assumes that the predictors are sorted by relevance in
	// descending order.
	// The "n" is defined in the predictor. Also the groupId is defined in the predictor.
	private static List<Predictor> getTopN(List<Predictor> predictorList, Predictor predictor) {
		List<Predictor> output = predictorList.stream().filter(p -> p.getGroupId()==predictor.getGroupId()).collect(Collectors.toList());
		
		return output.subList(0, Math.min(predictor.getPatternTopN(), output.size()));
	}
}

	