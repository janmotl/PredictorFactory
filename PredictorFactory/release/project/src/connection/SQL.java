package connection;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import run.Setting;
import utility.Meta;
import utility.Meta.Table;

import com.google.common.collect.Lists;

import featureExtraction.Predictor;

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
		
	// Subroutine 2.1: Expand tables (like: Table -> Schema.Table)
	private static String expandName(String sql){
	
		// While MySQL doesn't implement "true" schemas, it implements information_schema
		// and places it next to the database (instead of inside the database). 
		// Hence purely based on the hierarchical structure we call MySQL's database as schema.
		sql = sql.replace("@outputTable", "@outputSchema.@outputTable");
		sql = sql.replace("@inputTable", "@inputSchema.@inputTable");
		sql = sql.replace("@targetTable", "@inputSchema.@targetTable");
		sql = sql.replace("@baseTable", "@outputSchema.@baseTable");
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
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("Code is required");
		}
		if (StringUtils.isBlank(outputTable)) {
			throw new IllegalArgumentException("Output table is required");
		}
		if (StringUtils.isBlank(setting.targetId)) {
			throw new IllegalArgumentException("Id column is required");
		}
		if (StringUtils.isBlank(setting.baseTable)) {
			throw new IllegalArgumentException("Base table is required");
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
		if (StringUtils.isBlank(setting.baseId)) {
			throw new IllegalArgumentException("Base id is required");
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
		
		// Escape the entities
		sql = sql.replaceAll("\\@idColumn\\b", QL + setting.targetId + QR);	// There can be several numbered id columns
		sql = sql.replace("@baseId", QL + setting.baseId + QR);
		sql = sql.replace("@baseDate", QL + setting.baseDate + QR);
		sql = sql.replace("@baseTarget", QL + setting.baseTarget + QR);
		sql = sql.replace("@baseFold", QL + setting.baseFold + QR);
		sql = sql.replace("@baseTable", QL + setting.baseTable + QR);
		sql = sql.replace("@targetDate", QL + setting.targetDate + QR);
		sql = sql.replace("@targetColumn", QL + setting.targetColumn + QR);
		sql = sql.replace("@targetTable", QL + setting.targetTable + QR);
		sql = sql.replace("@inputSchema", QL + setting.inputSchema + QR);
		sql = sql.replace("@outputSchema", QL + setting.outputSchema + QR);
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
	
	// Subroutine: Is the predictor categorical? Just ask the database.
	// Note: The data type could be predicted from the pattern and pattern parameters. But the implemented 
	// method is foolproof, though slow.
	private static String getPredictorType(Setting setting, String table, String column) {
				
		SortedMap<String, Integer> allColumns = Meta.collectColumns(setting, setting.database, setting.outputSchema, table);
		SortedMap<String, Integer> columnMap = new TreeMap<String, Integer>();
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
	
	// Remove all Predictor Factory related tables
	// Note: The function is using names from the setting object. If the current setting doesn't match the setting
	// with witch the tables were generated, the tables are NOT going to get dropped! 
	// Note: Deleting whole schema is not going to work, if it contains tables (at least in PostgreSQL).
	// Note: We could delete all tables in the schema. But I am terrified of consequences, if someone with administrator
	// privileges entered wrong output schema (for example, if someone swaps by accident input and output schema).
	// Also, if someone has set up Predictor Factory that inputSchema=outputSchema, we would delete all the user's data.
	// Note: Use addBatch() for speeding up if possible. Take care of memory limits as described at:
	// http://viralpatel.net/blogs/batch-insert-in-java-jdbc/
	public static void tidyUp(Setting setting) {
		// Initialization
		SortedSet<String> tableSet = utility.Meta.collectTables(setting, setting.database, setting.outputSchema);
		SortedSet<String> dropSet = new TreeSet<String>(); 
		
		// Select tables for dropping
		for (String table : tableSet) {
			if (table.startsWith(setting.predictorPrefix)) dropSet.add(table);	// Predictors
			if (table.startsWith(setting.propagatedPrefix)) dropSet.add(table);	// Propagated tables
			if (table.startsWith(setting.sampleTable)) dropSet.add(table);		// Mainsample and it's temporary tables
			if (table.equals(setting.baseTable)) dropSet.add(table);			// Base table
			if ("base_sampled".equals(table)) dropSet.add(table);			// Base table
			if (table.equals(setting.journalTable)) dropSet.add(table);			// Journal table
		}

		// Drop the tables
		for (String table : dropSet) {
			dropTable(setting, table);
		}
	}
	
	// Create index on {baseId, baseDate}.
	// Returns true if the update was successful.
	public static boolean addIndex(Setting setting, String outputTable) {	
		
		String sql = "CREATE INDEX " + outputTable + "_idx ON @outputTable (@baseId, @baseDate)";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, outputTable);
		
		boolean isOK = Network.executeUpdate(setting.connection, sql);
		
		return isOK;
	}
	
	
	// Get rowCount
	public static int getRowCount(Setting setting, String inputTable) {
		String sql = "SELECT count(*) FROM @outputTable";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, inputTable);
		
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
	public static int getNotNullCount(Setting setting, String table, String column) {
		// By default count over a column ignores NULL values
		String sql = "SELECT count(@column) FROM @outputTable";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);
		
		Map<String, String> fieldMap = new HashMap<String, String>(); 
		fieldMap.put("@column", column);
		sql = escapeEntityMap(setting, sql, fieldMap);
		
		List<String> resultList = Network.executeQuery(setting.connection, sql);
		
		// If the table doesn't exist, the resultSet is empty. Return 0.
		if (resultList.isEmpty()) return 0;
		
		return (int)Double.parseDouble(resultList.get(0)); // SAS can return 682.0
	}
	
	// Get the maximal cardinality of the table in respect to idColumn. If the cardinality is 1:1, 
	// we may want to remove the bottom time constrain in base propagation.
	// The map should contain @inputTable and @idColumn2.
	// The indexes start at two because it was convenient to reuse the parameters in Propagation function.
	// Note that we are working with the input tables -> alter commands are forbidden.
	// It was difficult to write a version that would be more effective than sum(... having count(*)>1).
	// If you decide to replace this query, test it with several database engines!
	public static boolean isIdUnique(Setting setting, Map<String, String> map) {
		String sql = "SELECT count(*) " +
					 "FROM @inputTable " +
					 "GROUP BY @idColumn2 " +
					 "HAVING COUNT(*)>1"; 			
				
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityMap(setting, sql, map);
		
		boolean result = Network.isResultSetEmpty(setting.connection, sql);
		
		if (result) logger.trace("# Column " + map.get("idColumn2") + " in " + map.get("inputTable") + " doesn't contain duplicates #");
		else logger.trace("# Column " + map.get("idColumn2") + " in " + map.get("inputTable") + " CONTAINS duplicates #");
		
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
					 "GROUP BY @idColumn, @targetDate " +
					 "HAVING COUNT(*)>1";
		} else {
			sql = "SELECT count(*) " +
					 "FROM @outputTable " +
					 "GROUP BY @baseId, @baseDate " +
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
	
	// Subsample base table based on target class.
	// Works only for classification! For sampling of regression problem neglect the target OR discretize the target. 
	// Note: The selection is not guaranteed to be random.
	// NOTE: BASE_SAMPLED NAME SHOULD BE A VARIABLE
	public static void getSubSample(Setting setting, SortedMap<String, Table> metaInput) {
		
		// Initialization
		String sql = "";
		List<String> targetValueList = metaInput.get(setting.targetTable).uniqueList.get(setting.targetColumn);
		String quote = "";
		
		// Iff the target is nominal, quote the values with single quotes.
		if (setting.isTargetNominal) {
			quote = "'";
		}
		
		// Create union 
		for (int i = 0; i < targetValueList.size(); i++) {
			sql = sql + "(" + Parser.limitResultSet(setting, "SELECT * FROM @outputSchema.base WHERE @baseTarget = " + quote + targetValueList.get(i) + quote + "\n", setting.sampleCount) + ")"; 
			sql = sql + " UNION ALL \n";	// Add "union all" between all the selects.
		}
		
		// Finally, add unclassified records.
		sql = sql + "(" + Parser.limitResultSet(setting, "SELECT * FROM @outputSchema.base WHERE @baseTarget is null\n", setting.sampleCount) + ")";
						
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "base_sampled");
		
		Network.executeUpdate(setting.connection, sql);
		
		// Change setting for base table
		setting.baseTable = "base_sampled";
		
		// Add indexes
		addIndex(setting, "base_sampled");
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
	
	// Get R2. For discrete variables, use the method as described at:
	// http://stats.stackexchange.com/questions/119835/correlation-between-a-nominal-iv-and-a-continuous-dv-variable 
	// IMPLEMENTED FOR POSTGRESQL. TEST ON OTHER DATABASES.
 	public static double getR2(Setting setting, String table, String column) {
 		// Initialization
 		String sql;
 		String dataType = getPredictorType(setting, table, column);
 		
 		// Is the predictor categorical, numeric or time? 		
 		if ("nominal".equals(dataType)) {
 			sql = "select corr(t2.average, t1.@baseTarget)^2 " +
					"from @outputTable t1 " +
					"join ( " +
						"select @column " + 
							", avg(@baseTarget) as average " +
						"from @outputTable " +
						"group by @column " +
					") t2 " +
					"on t1.@column = t2.@column";
 		} else if ("numerical".equals(dataType)){
 			// "SELECT (Avg(@column * @baseTarget) - Avg(@column) * Avg(@baseTarget)) / (stdDev_samp(@column) * stdDev_samp(@baseTarget)) AS Correlation "
 			
 			sql = "SELECT corr(@column, @baseTarget)^2 " +
					 "FROM @outputTable "; /* We are working with the outptutTable, hence schema & database are output. */
 			
 			// Take care of stdDev
 			//sql = sql.replaceAll("stdDev_samp", setting.stdDevCommand);
 		} else {
 			sql = "SELECT corr(EXTRACT(epoch FROM @column), @baseTarget)^2 " +
					 "FROM @outputTable ";
 		}
 		
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
			logger.info("The response in correlation calculation is null (empty table...), return 0.");
			correlation = 0;
		}
		
		return correlation;
	}
	
 	// Get Chi2.
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
 			sql = ""
				+ "select sum(chi2) "
				+ "from ( "
				+ "	select (expected.expected-measured.count) * (expected.expected-measured.count) / expected.expected AS chi2 "
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
 			sql = ""
				+ "select sum(chi2) "
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
 	
 	
 	
 	
	// 1) Get base table (a table with id, targets and horizon dates).
 	// The base table could be practical, because we may simply add random sample column.
 	// Return true if the base table was successfully created.
 	public static boolean getBase(Setting setting) {
 		logger.debug("# Setting up base table #");
 		
		String sql;
		
		// Detect duplicates in the base table
		boolean isUnique = isUnique(setting, setting.targetTable, true);
		
		// Deduplicate the base table if necessary
		if (!isUnique) {
			logger.warn("The base table contains duplicate values in {BaseID, BaseDate}. " +
					"Continuing without ALL duplicate values. " +
					"The results will be uncomplete and possibly biased. " +
					"To get correct results create an artificial key / use time column with higher precission...");
		
			sql = "SELECT @idColumn AS " + setting.baseId + ", @targetDate AS " + setting.baseDate + ", @targetColumn AS " + setting.baseTarget + ", FLOOR(" + setting.randomCommand + " * 10) AS " + setting.baseFold + " " +
					"FROM @targetTable t1 LEFT JOIN (" +
					"SELECT @idColumn FROM @targetTable GROUP BY @idColumn, @targetDate HAVING count(*)>1 " +
					") t2 " +
					"ON t1.@idColumn = t2.@idColumn " +
					"WHERE t2.@idColumn is null";
		} else {
			sql = "SELECT @idColumn AS " + setting.baseId + ", @targetDate AS " + setting.baseDate + ", @targetColumn AS " + setting.baseTarget + ", FLOOR(" + setting.randomCommand + " * 10) AS " + setting.baseFold + " FROM @targetTable";
		}
		
		// Assembly the query
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.baseTable);
		
		// Execute the query
		boolean isCreated = Network.executeUpdate(setting.connection, sql);
		
		if (!isCreated) {
			logger.warn("The base table was not sussesfully created.");
		}
		
		// Add index
		SQL.addIndex(setting, setting.baseTable);
		
		return (isUnique && isCreated);
	}
	
	// 2) Propagate ID. The map should contain @outputTable, @propagatedTable, @inputTable and @idColumn[?].
	// If the map contains @dateColumn, time condition is added.
	// Technical note: We have to return SQL string, because these things are logged and exported for the user
 	// as the "scoring code" for predictors.
	public static String propagateID(Setting setting, Map<String, String> map, boolean bottomBounded){
				
		String sql = "SELECT t1.@baseId, " +
				"t1.@baseDate, " +
				"t1.@baseTarget, " +
				"t1.@baseFold, " +
				"t2.* " + 
				"FROM @propagatedTable t1 " +
				"INNER JOIN @inputTable t2 " +
				"ON t1.@idColumn1 = t2.@idColumn2";
		
		// Add time condition if dateColumn is present
		// The comparison "t2.@dateColumn <= t1.@baseDate" has to use <= to get 
		// the data from "the current date" when lead is 0.

		if (map.containsKey( "@dateColumn")) {
			// First the upper bound (lead)
			sql = sql + " WHERE t2.@dateColumn <= " + setting.dateAddMonth;
			sql = sql.replaceAll("@amount", "-" + setting.lead.toString()); // Negative lead		
			
			// Then, if required, add the lower bound (lag)
			if (bottomBounded) {
				sql = sql + " AND " + setting.dateAddMonth + " <= t2.@dateColumn";
				Integer leadLag = setting.lead + setting.lag;
				sql = sql.replaceAll("@amount", "-" + leadLag.toString()); // Negative lead+lag
			} 
		}
						
		// Pattern_code to SQL conversion
		sql = addCreateTableAs(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, map.get("@outputTable"));
		sql = escapeEntityMap(setting, sql, map);
					
		return sql;
	}
	
	// 3a) Return create journal_predictor table command
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
	 
	// 3b) Add record into the journal_predictor
	// Return true if the journal table was successfully updated.
	public static boolean addToJournal(Setting setting, Predictor predictor) {
		DateTimeFormatter formatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
		
		// Convert bool to int
		int isOk = predictor.isOk()? 1 : 0;
		
		// Insert timestamp subquery
		String template = setting.insertTimestampSyntax;
		String timestamp = predictor.getTimestampBuilt().format(formatter);
		timestamp = template.replace("@timestamp", timestamp);
		
		// Assembly the insert
		String sql = "INSERT INTO @outputTable VALUES (" +
	        predictor.getId() + ", " +
	        predictor.getGroupId() + ", " +
	              timestamp + ", " +
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
			      predictor.getRelevance().get(setting.baseTarget) + ", " + // Chi2
	              predictor.getRowCount() + ", " + 
	              predictor.getNullCount() + ", " + 
	              isOk + ")";
		
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, setting.journalTable);

		return Network.executeUpdate(setting.connection, sql);
	}
	
	// 4) Get predictor
	public static String getPredictor(Setting setting, Predictor predictor){
		String sql;
		
		sql = addCreateTableAs(setting, predictor.getSql());  // The patternCode could be modified by parameters -> use SQL 
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, predictor.outputTable);	
		sql = escapeEntityPredictor(setting, sql, predictor);		
		
		if ("SAS".equals(setting.databaseVendor)) {
			sql = sql.replace("\"", "");				// VERY UGLY wORKAROUND. SHOULD IMPLEMENT quoteAlias
		}
		
		return sql;
	}
	
	// 5) Assembly the final step - the output table
	// Note: The current implementation stores only up to ~3600 predictors. So far the limit is acceptable as column 
	// count in a table is commonly limited (1600 columns in PostgreSQL and 1000 columns in Oracle).
	public static void getMainSample(Setting setting, List<Predictor> journal) {
		
		// Consider only good predictors 
		List<Predictor> predictorList = journal.stream().filter(p -> p.isOk()).collect(Collectors.toList());
				
		// Sort the relevances in descending order
		Collections.sort(predictorList, Predictor.RelevanceComparator.reversed());
		
		// Keep top N predictors per groupId
		Collections.sort(predictorList, Predictor.GroupIdComparator);
		
		int lagGroupId = 0;
		List<Predictor> predictorList2 = new ArrayList<Predictor>();
		
		for (Predictor predictor : predictorList) {
			if (predictor.getGroupId() != lagGroupId) {
				predictorList2.addAll(getTopN(predictorList, predictor));
			}
			lagGroupId = predictor.getGroupId();
		}
		

		// Cap the amount of predictors by columnMax
		// RESERVE 3 COLUMNS FOR ID, TARGET AND TIME
		// THE COUNT OF COLUMNS IS ALSO LIMITED BY ROW SIZE. AND BIGINT COMPOSES OF 8 BYTES -> DIVIDE BY 8.
		predictorList = predictorList2.subList(0, Math.min(setting.columnMax/8-3, predictorList2.size()));
		
		// Extract table and column names.
		// ASSUMING THAT THE MATCH IS 1:1!
		List<String> tableListAll = new ArrayList<String>();
		List<String> columnListAll = new ArrayList<String>();
		
		for (Predictor predictor : predictorList) {
				tableListAll.add(predictor.outputTable);
				columnListAll.add(predictor.getName());
		}
		
		// MySQL can join maximally 61 tables in a single command. Hence split the task into several smaller joins.
		List<List<String>> tableListSmall = Lists.partition(tableListAll, 60); // One table is reserved for baseTable. 
		List<List<String>> columnListSmall = Lists.partition(columnListAll, 60);
		
		// Prepare a list of the temporary table names
		List<String> tempTableList = new ArrayList<String>();
		
		// Create temporary tables
		for (int i = 0; i < tableListSmall.size(); i++) {
			// Initialization
			StringBuilder stringBuffer = new StringBuilder(500);
			int tableCount;													// The tables are named t1..t* in the join
			String tempTable = setting.sampleTable + "_temp" + (100+i); 	// The name of the temporary table
			tempTableList.add(tempTable);
			List<String> tableList = tableListSmall.get(i);					
			List<String> columnList = columnListSmall.get(i);			
			
			// Select part 
			stringBuffer.append("SELECT t1.@baseId, t1.@baseDate, t1.@baseTarget");
			tableCount = 2;
			for (String column : columnList) {
				stringBuffer.append(", t" + tableCount + ".@"  + column);	// The column name will be escaped
				tableCount++;
			}
			
			// From part
			stringBuffer.append(" FROM @baseTable t1");
			tableCount = 2;
			for (String table : tableList) {
				stringBuffer.append(" LEFT JOIN " + table + " t" + tableCount +
						" ON t1.@baseId = t" + tableCount + ".@baseId AND t1.@baseDate = t" + tableCount + ".@baseDate");
				tableCount++;
			}
			
			// Make SQL from the pattern
			String pattern_code = stringBuffer.toString();
			pattern_code = addCreateTableAs(setting, pattern_code);
			pattern_code = expandName(pattern_code);
			pattern_code = expandNameList(pattern_code, tableList);
			pattern_code = escapeEntity(setting, pattern_code, tempTable); 
			
			// Escape table & column entities (tables can't be escaped in definition because they have to be first expanded...)
			Map<String, String> map = new HashMap<String, String>(61);
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
		stringBuffer.append("SELECT t1.@baseId, t1.@baseDate, t1.@baseTarget");
		
		for (int i = 0; i < tempTableList.size(); i++) {
			for (String column : columnListSmall.get(i)) {
				stringBuffer.append(", t" + (i+2) + ".@"  + column);	// The column name will be escaped
			}	
		}
		
		// From part
		stringBuffer.append(" FROM @baseTable t1");

		for (int i = 0; i < tempTableList.size(); i++) {
			int tableCount = i+2;
			String tempTable = tempTableList.get(i);
			stringBuffer.append(" INNER JOIN " + tempTable + " t" + tableCount +
					" ON t1.@baseId = t" + tableCount + ".@baseId AND t1.@baseDate = t" + tableCount + ".@baseDate");
		}
		
		// Make SQL from the pattern
		String pattern_code = stringBuffer.toString();
		pattern_code = addCreateTableAs(setting, pattern_code);
		pattern_code = expandName(pattern_code);
		pattern_code = expandNameList(pattern_code, tempTableList);
		pattern_code = escapeEntity(setting, pattern_code, setting.sampleTable); 
		
		// Escape table & column entities (tables can't be escaped in definition because they have to be first expanded...)
		Map<String, String> map = new HashMap<String, String>(61);
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
		
		int columnCount = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable).size();
		logger.debug("MainSample table contains: " + columnCount + " columns (the limit is: " + (setting.columnMax/8) + ")");
	} 
	
	// Subroutine - return top n predictors from the list
	private static List<Predictor> getTopN(List<Predictor> predictorList, Predictor predictor) {
		List<Predictor> output = predictorList.stream().filter(p -> p.getGroupId()==predictor.getGroupId()).collect(Collectors.toList());
		
		return output.subList(0, Math.min(predictor.getPatternTopN(), output.size()));
	}
}

	