package utility;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

import run.Predictor;
import run.Setting;

public final class SQL {

	// Subroutine 1: Add "Create table as" sequence into the pattern
	private static String addCreateTableAs(Setting setting, String pattern_code) {
		String result;

		// If not specified, the default is "Create table as"
		if (!setting.isCreateTableAsCompatible) {
			result = pattern_code.replace("FROM", "INTO @outputTable FROM");
		} else {
			result = "CREATE TABLE @outputTable AS " + pattern_code;
		}

		return result;
	}
		
	// Subroutine 2.1: Expand tables (like: Table -> Database.Schema.Table)
	private static String expandName(Setting setting, String pattern_code){
		String patternCode = pattern_code;
		
		if (setting.isSchemaCompatible){
			patternCode = patternCode.replace("@outputTable", "@outputDatabase.@outputSchema.@outputTable");
			patternCode = patternCode.replace("@inputTable", "@inputDatabase.@inputSchema.@inputTable");
			patternCode = patternCode.replace("@targetTable", "@inputDatabase.@inputSchema.@targetTable");
			patternCode = patternCode.replace("@idTable", "@inputDatabase.@inputSchema.@idTable");
			patternCode = patternCode.replace("@baseTable", "@outputDatabase.@outputSchema.@baseTable");
		} else {
			// While MySQL doesn't implement "true" schemas, it implements information_schema
			// and places it next to the database (instead of inside the database). 
			// Hence purely based on the hierarchical structure we call MySQL's database as schema.
			patternCode = patternCode.replace("@outputTable", "@outputSchema.@outputTable");
			patternCode = patternCode.replace("@inputTable", "@inputSchema.@inputTable");
			patternCode = patternCode.replace("@targetTable", "@inputSchema.@targetTable");
			patternCode = patternCode.replace("@idTable", "@inputSchema.@idTable");
			patternCode = patternCode.replace("@baseTable", "@outputSchema.@baseTable");
		}
		
		return patternCode;
	}
	
	// Subroutine 2.2: Expand tables based on list (like: Table -> Database.Schema.Table)
	private static String expandNameList(Setting setting, String pattern_code, List<String> list) {
		String patternCode = pattern_code;

		if (setting.isSchemaCompatible) {
			for (String field : list) {
				patternCode = patternCode.replace(field, "@outputDatabase.@outputSchema." + field);
			}
		} else {
			// While MySQL doesn't implement "true" schemas, it implements information_schema
			// and places it next to the database (instead of inside the database).
			// Hence purely based on the hierarchical structure we call MySQL's database as schema.
			for (String field : list) {
				patternCode = patternCode.replace(field, "@outputSchema." + field);
			}
		}

		return patternCode;
	}
	
	// Subroutine 3.1: Replace & escape the entities present in setting
	private static String escapeEntity(Setting setting, String pattern_code, String outputTable) {
		// Test parameters
		if (StringUtils.isBlank(pattern_code)) {
			throw new IllegalArgumentException("Code is required");
		}
		if (StringUtils.isBlank(outputTable)) {
			throw new IllegalArgumentException("Output table is required");
		}
		if (StringUtils.isBlank(setting.idColumn)) {
			throw new IllegalArgumentException("Id column is required");
		}
		if (StringUtils.isBlank(setting.idTable)) {
			throw new IllegalArgumentException("Id table is required");
		}
		if (StringUtils.isBlank(setting.baseTable)) {
			throw new IllegalArgumentException("Base table is required");
		}
		if (StringUtils.isBlank(setting.targetDate)) {
			throw new IllegalArgumentException("Target date is required");
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


		String QL = setting.quoteMarks.substring(0, 1);
		String QR = setting.quoteMarks.substring(1, 2);
		String sql;

		sql = pattern_code.replaceAll("@idTable", QL + setting.idTable + QR);
		sql = sql.replaceAll("\\@idColumn\\b", QL + setting.idColumn + QR);	// There can be several numbered id columns
		sql = sql.replaceAll("@baseId", QL + setting.baseId + QR);
		sql = sql.replaceAll("@baseDate", QL + setting.baseDate + QR);
		sql = sql.replaceAll("@baseTarget", QL + setting.baseTarget + QR);
		sql = sql.replaceAll("@baseTable", QL + setting.baseTable + QR);
		sql = sql.replaceAll("@targetDate", QL + setting.targetDate + QR);
		sql = sql.replaceAll("@targetColumn", QL + setting.targetColumn + QR);
		sql = sql.replaceAll("@targetTable", QL + setting.targetTable + QR);
		sql = sql.replaceAll("@inputSchema", QL + setting.inputSchema + QR);
		sql = sql.replaceAll("@outputSchema", QL + setting.outputSchema + QR);
		sql = sql.replaceAll("@inputDatabase", QL + setting.inputDatabaseName + QR);
		sql = sql.replaceAll("@outputDatabase", QL + setting.outputDatabaseName + QR);
		sql = sql.replaceAll("@outputTable", QL + outputTable + QR);
		
		// Make sure the command is terminated with the semicolon. This is useful for example if we want 
	    // to move SQL commands from the journal to the production. 
	    if (!sql.endsWith(";")) sql = sql +";";
		
		return sql;
	}

	// Subroutine 3.2: Replace & escape the entities from predictor fields
	private static String escapeEntityPredictor(Setting setting, String pattern_code, Predictor predictor) {
	    // Test parameters
	    if (StringUtils.isBlank(pattern_code)) {
	      throw new IllegalArgumentException("Code is required");
	    }
	    if (predictor.columnMap==null) {
		  throw new IllegalArgumentException("ColumnMap can not be null, but it can be empty");
		}
	    if (StringUtils.isBlank(predictor.inputTable)) {
	      throw new IllegalArgumentException("InputTable is required");
	    }

	    String QL = setting.quoteMarks.substring(0, 1);
	    String QR = setting.quoteMarks.substring(1, 2);
	    String sql;

	    sql = pattern_code.replace("@inputTable", QL + predictor.inputTable + QR);
	    sql = sql.replace("@columnName",  predictor.getName());
	    
	    for (String columnName : predictor.columnMap.keySet()) {
	    	sql = sql.replace(columnName, QL + predictor.columnMap.get(columnName) + QR);
		}
	    
	    for (String parameterName : predictor.getParameterList().keySet()) {
	      sql = sql.replace(parameterName, predictor.getParameterList().get(parameterName));
	    }
	    
	    // Make sure the command is terminated with the semicolon. This is useful for example if we want 
	    // to move SQL commands from the journal to the production. 
	    if (!sql.endsWith(";")) sql = sql +";";

	    return sql;
	  }
	
	// Subroutine 3.3: Replace & escape the entities from map
	private static String escapeEntityMap(Setting setting, String sql, Map<String, String> fieldMap) {
		// Test parameters
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("Code is required");
		}
		
		String QL = setting.quoteMarks.substring(0, 1);
		String QR = setting.quoteMarks.substring(1, 2);
	
		for (Map.Entry<String, String> field : fieldMap.entrySet()) {
			sql = sql.replace(field.getKey(), QL + field.getValue() + QR);
		}
		
		// Make sure the command is terminated with the semicolon. This is useful for example if we want 
	    // to move SQL commands from the journal to the production. 
	    if (!sql.endsWith(";")) sql = sql +";";

		return sql;
	}
	
	
	
	
	
	// Drop command
	// SHOULD EXECUTE DROP AND JUST RETURN BOOLEAN
	public static String getDropTable(Setting setting, String outputTable) {
		// Test parameters
		if (StringUtils.isBlank(outputTable)) {
			throw new IllegalArgumentException("Output table is required");
		}
		
		String sql;
		
		String pattern_code = "DROP TABLE @outputTable";
		pattern_code = expandName(setting, pattern_code);
		sql = escapeEntity(setting, pattern_code, outputTable);
		
		return sql;
	}
	
	// Remove all Predictor Factory related tables
	// Note: The function is using names from the setting object. If the current setting doesn't match the setting
	// with witch the tables were generated, the tables are NOT going to get dropped! 
	public static void tidyUp(Setting setting) {
		List<String> tableList;
		
		// Propagated tables
		tableList = Network.executeQuery(setting.connection, getTableList(setting, true));
		for (String table : tableList) {
			Network.executeUpdate(setting.connection, getDropTable(setting, table));
		}
		
		// Predictors
		tableList = getTableList(setting, "TABLE_NAME like '" + setting.predictorPrefix +"%'");
		for (String table : tableList) {
			Network.executeUpdate(setting.connection, getDropTable(setting, table));
		}
		
		// MainSample
		tableList = getTableList(setting, "TABLE_NAME like '" + setting.sampleTable +"%'");
		for (String table : tableList) {
			Network.executeUpdate(setting.connection, getDropTable(setting, table));
		}
		
		// Base table
		Network.executeUpdate(setting.connection, getDropTable(setting, setting.baseTable));
		
		// Journal table
		Network.executeUpdate(setting.connection, getDropTable(setting, setting.journalTable));
		
		// Statement table
		Network.executeUpdate(setting.connection, getDropTable(setting, setting.statementTable));
		
	}
	
	// Assembly create index
	// WILL BE PRIVATE
	public static String getIndex(Setting setting, String outputTable) {	
		// The index name must be unique per table, but it doesn't have to be unique per schema.
		String sql = "CREATE INDEX baseId_index ON @outputTable (@baseId)";  
		
		// I want to try what happens if I use a constant instead of variable index name
		//Map<String, String> map = new HashMap<String, String>(1);
		//map.put("@indexName", setting.baseId + "_index");
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, outputTable);
		//sql = escapeEntityMap(setting, sql, map);
		
		return sql;
	}
	
	// Get tableList based on own where condition
    public static List<String> getTableList(Setting setting, String whereClause) {

		String sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA like '" + 
					 setting.inputSchema + 
					 "' ORDER BY TABLE_NAME";

		// Add where condition/conditions
		sql = sql.replace("WHERE", "WHERE " + whereClause + " AND");
		
    	// Add database name?
    	if (setting.isSchemaCompatible){
    		sql = sql.replace("information_schema", setting.inputDatabaseName + ".information_schema");
    	}

    	return Network.executeQuery(setting.connection, sql);
    }

	// Assembly getTableList query
    // SHOULD RETURN THE TABLELIST/SORTEDSET
    public static String getTableList(Setting setting, boolean propagated){
    	String sql;
    	
    	// Do we want unpropagated or propagated tables?
    	String propagatedString;
    	if (propagated) {
    		propagatedString = " AND TABLE_NAME LIKE '" + setting.propagatedPrefix + "%"; 
    	} else {
    		propagatedString = " AND TABLE_NAME NOT LIKE '" + setting.propagatedPrefix + "%";
    	}

    	// Ignore meta tables 
		sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA like '" + setting.inputSchema + 
				"' AND TABLE_NAME not like '" + setting.predictorPrefix + "%" +
				"' AND TABLE_NAME not like '" + setting.journalTable +
				"' AND TABLE_NAME not like '" + setting.statementTable +
				"' AND TABLE_NAME not like '" + setting.baseTable +
				"' AND TABLE_NAME not like '" + setting.sampleTable + "%" +	// MainSample and it's temporary tables 
				"' AND TABLE_NAME not in " + setting.blackList +
				propagatedString + 
				"' ORDER BY TABLE_NAME";

    	// Add database name?
    	if (setting.isSchemaCompatible){
    		sql = sql.replace("information_schema", setting.inputDatabaseName + ".information_schema");
    	}

		return sql;
	}

    
    // Return list of columns in given table
    // The allowed parameters for dataType are {number, string, date}
	public static SortedSet<String> getColumnList(Setting setting, String inputTable, String dataType){
		String sql; 
		String condition = "";
		
		// By default there is not any condition.
		switch (dataType.toUpperCase()) {
		case "NUMBER":
			condition = " AND NUMERIC_PRECISION IS NOT NULL";
			break;
		case "STRING":
			condition = " AND CHARACTER_SET_NAME IS NOT NULL";
			break;
		case "DATE":
			// I believe this condition is more generic than: " AND (DATA_TYPE='date' or DATA_TYPE='datetime')"
			condition = " AND CHARACTER_SET_NAME IS NULL AND NUMERIC_PRECISION IS NULL";
		}

	
		if (setting.isSchemaCompatible){
    		sql = "SELECT COLUMN_NAME FROM " + setting.inputDatabaseName + ".information_schema.COLUMNS" + 
    			  " WHERE TABLE_CATALOG='" + setting.inputDatabaseName + 
    			  "' AND TABLE_SCHEMA='" + setting.inputSchema + 
    			  "' AND TABLE_NAME='" + inputTable +"'" + 					// Define table
    			  " AND COLUMN_NAME not like '" + setting.baseTarget + "'" +	// Ignore propagated_target
    			  " AND COLUMN_NAME not like '" + setting.baseDate+ "' " +		// Ignore propagated_date
    			  " AND COLUMN_NAME not like '" + setting.baseId+ "' " +		// Ignore propagated_id
    			  " AND not (TABLE_NAME like '" + setting.propagatedPrefix + setting.targetTable + "' AND COLUMN_NAME like '" + setting.targetColumn + "')" + // Ignore target column
    			  condition + 													// Numbers, dates or strings?
    			  " ORDER BY COLUMN_NAME";
    	} else {
    		sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS" +
    			  " WHERE TABLE_SCHEMA='" + setting.inputSchema + 				// Look for ordinary tables, not information_schema
    			  "' AND TABLE_NAME='" + inputTable + "'" + 					// Define table
    			  " AND COLUMN_NAME not like '" + setting.baseTarget + "'" +	// Ignore propagated_target
    			  " AND COLUMN_NAME not like '" + setting.baseDate+ "' " +		// Ignore propagated_date
    			  " AND COLUMN_NAME not like '" + setting.baseId+ "' " +		// Ignore propagated_id
    			  " AND not (TABLE_NAME like '" + setting.propagatedPrefix + setting.targetTable + "' AND COLUMN_NAME like '" + setting.targetColumn + "')" + // Ignore target column
    			  condition + 													// Numbers, dates or strings?
    			  " ORDER BY COLUMN_NAME";
    	}
		
		// Do not escape COLUMNS entity. PostgreSQL refuses (any type of) quotes in information_schema.
		
		// There shall not be duplicates 
		SortedSet<String> result = new TreeSet<String>(Network.executeQuery(setting.connection, sql));
		
		return result;
	}
    
    
	// Return columns shared between two tables (based on name)
	// NOTE THAT THIS IS NOT A RELIABLE WAY HOW TO IDENTIFY LINKS BETWEEN TABLES.
	public static ArrayList<String> getSharedColumns(Setting setting, String inputTable, String inputTable2) {
		String sql = "SELECT t2.COLUMN_NAME " + 
					 "FROM information_schema.COLUMNS t1 " +
					 "INNER JOIN information_schema.COLUMNS t2 " +		// MySQL doesn't support intersect
					 "WHERE t1.TABLE_SCHEMA = '" + setting.inputSchema + "' " +
					 "AND t1.TABLE_NAME = '" + inputTable + "' " +
					 "AND t2.TABLE_SCHEMA = '" + setting.inputSchema + "' " +
					 "AND t2.TABLE_NAME = '" + inputTable2 + "' "  +
					 "AND (t1.COLUMN_NAME = t2.COLUMN_NAME OR (" +
					 	"t1.COLUMN_NAME = '" + setting.baseId + "' AND t2.COLUMN_NAME = '" + setting.idColumn +"'))"; // The baseTable exception
		
		ArrayList<String> columnList = Network.executeQuery(setting.connection, sql);
		
		return columnList;
	}
	
	
	// Get rowCount
	// SHOULD RETURN INT
	public static String getRowCount(Setting setting, String inputTable) {
		String sql = "SELECT count(*) FROM @outputTable";
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, inputTable);
		
		// Dirt cheap approach how to get row count is unfortunately database/driver specific. Hence universal count(*) 
		// is implemented. Some databases, like MySQL, returns the answer to count(*) query immediately. In other 
		// databases, like PostgreSQL, you may have to wait ~200ms.
		
		return sql; 
	}
	
	// Get count of nulls
	// Useful for QC of the predictors
	public static int getNullCount(Setting setting, String table, String column) {
		String sql = "SELECT sum(@column is null) FROM @outputTable";
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, table);
		
		Map<String, String> fieldMap = new HashMap<String, String>(); 
		fieldMap.put("@column", column);
		sql = escapeEntityMap(setting, sql, fieldMap);
		
		ArrayList<String> resultList = Network.executeQuery(setting.connection, sql);
		
		return Integer.valueOf(resultList.get(0)).intValue(); // Can return NullPointerException
	}
	
	// Get the maximal cardinality of the table in respect to idColumn. If the cardinality is 1:1, 
	// we may want to remove the bottom time constrain in base propagation.
	// The map should contain @inputTable2 and @idColumn2.
	// The indexes start at two because it was convenient to reuse the parameters in Propagation function.
	// Note that we are working with the input tables -> alter commands are forbidden.
	// WOULD BE UNOUGH TO FIND EXISTENCE OF A DUPLICATE KEY IN TABLE2 - USE LIMIT/TOP/ROWNUM
	public static int getIdCardinality(Setting setting, Map<String, String> map) {
		String sql = "SELECT max(count) FROM (" + 
						"SELECT count(*) as count " +
						"FROM @inputTable2 " +
						"GROUP BY @idColumn2" + 
					 ") as cardinality";
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityMap(setting, sql, map);
		
		ArrayList<String> resultSet = Network.executeQuery(setting.connection, sql);
		
		// Take care of the cases when the resultSet is empty
		int maxCardinality = 0;
		if (!resultSet.isEmpty()) {
			maxCardinality = Integer.valueOf(resultSet.get(0));
		}
		
		return maxCardinality;
	}

	// Check whether the columns {baseId, baseDate} are unique in the table.
	// If the columns are unique, we may avoid of aggregation and copy the values immediately. 
	// Note that in comparison to getIdCardinality we can modify the tables as we are working with outputSchema. 
	public static boolean isUnique(Setting setting, String table) {
		String sql = "ALTER TABLE @outputTable " + 
					 "ADD UNIQUE (@baseId, @baseDate)";
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, table);
					
		return Network.executeUpdate(setting.connection, sql);	// Returns false at the first violation -> fast
	}
	
	// Get maximal cardinality of the table in respect to {baseId, baseDate}. If the cardinality is 1, 
	// we may avoid of aggregation and copy the values immediately. 
	// The map should contain @inputTable.
	// WOULD BE UNOUGH TO FIND EXISTENCE OF A DUPLICATE KEY IN TABLE2 - USE LIMIT/TOP/ROWNUM
	public static int getPropagationCardinality(Setting setting, Map<String, String> map) {
		String sql = "SELECT max(count) FROM (" + 
						"SELECT count(*) as count " +
						"FROM @inputTable " +
						"GROUP BY @baseId, @dateId" + 
					 ") as cardinality";
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityMap(setting, sql, map);
		
		ArrayList<String> resultSet = Network.executeQuery(setting.connection, sql);
		
		// Take care of the cases when the resultSet is empty
		int maxCardinality = 0;
		if (!resultSet.isEmpty()) {
			maxCardinality = Integer.valueOf(resultSet.get(0));
		}
		
		return maxCardinality;
	}
	
	// Return list of unique records in the column sorted descendingly based on record count.
	// This function is useful for example for dummy coding of nominal attributes.
	public static ArrayList<String> getTopRecords(Setting setting, String tableName, String columnName) {
		String sql = "SELECT @columnName" +
					 "FROM @outputTable" + 
					 "GROUP BY 1 " +
					 "ORDER BY count(*) DESC";
		
		sql = expandName(setting, sql);
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("@outputTable", tableName);
		map.put("@columnName", columnName);
		sql = escapeEntityMap(setting, sql, map);
		
		return Network.executeQuery(setting.connection, sql);
	}
	
	// Get relevance
	// CORRELATION IS JUST A TEMPORARY MEASURE, WHICH SHALL BE REPLACED WITH CHI2/UNCERTAINTY
	// DOESN'T WORK ON DISCRETE VALUES
	// JOINS ARE TOO SLOW. Calculate the worthness after making "All table's predictors". 
	// or add target into predictors' tables.
 	public static double getRelevance(Setting setting, String table, String column) {
		String sql = "SELECT (Avg(@column * @baseTarget) - Avg(@column) * Avg(@baseTarget)) / (StD(@column) * StD(@baseTarget)) AS Correlation " +
					 "FROM @outputTable t1 " + /* We are working with the outptutTable, hence schema & database are output. */
					 "JOIN @baseTable t2 ON t1.@column = t2.@baseTarget";
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, table);	/* outputTable, outputSchema, output.Database */
		
		// Escape the entities
		HashMap<String, String> fieldMap = new HashMap<String, String>();
		fieldMap.put("@column", column);
		fieldMap.put("@baseTarget", setting.baseTarget);
		sql = escapeEntityMap(setting, sql, fieldMap);
		
		// Execute the SQL
		ArrayList<String> response = Network.executeQuery(setting.connection, sql);
		
		// Parse the response
		double correlation;

		try {
			correlation = Double.valueOf(response.get(0)); 
		} catch (Exception e){
			correlation = 0;
		}
		
		
		return correlation;
	}
	
	
	
 	
	// 1) Get base table (a table with id, targets and horizon dates)
	// NEEDS finishing (what if the tables are far away from each other?)
	public static String getBase(Setting setting) {
		String sql;
		
		String pattern_code = "SELECT t1.@idColumn @baseId, t2.@targetDate @baseDate, t2.@targetColumn @baseTarget FROM @idTable t1 INNER JOIN @targetTable t2 ON t1.@idColumn = t2.@idColumn";
		pattern_code = addCreateTableAs(setting, pattern_code);
		pattern_code = expandName(setting, pattern_code);
		sql = escapeEntity(setting, pattern_code, setting.baseTable);
		
		return sql;
	}
	
	// 2) Propagate ID. The map should contain @outputTable, @inputTable[?] and @idColumn[?].
	// If the map contains @dateColumn, time condition is added.
	// THE TIME WINDOW SHOULD BE A VARIABLE
	// ADD SAFETY "TRANSITION BAND"  
	public static boolean propagateID(Setting setting, Map<String, String> map, boolean bottomBounded){
		String sql;
				
		String pattern_code = "SELECT t1.@baseId, " +
				"t1.@baseDate, " +
				"t1.@baseTarget, " +
				"t2.* " + 
				"FROM @inputTable1 t1 " +
				"INNER JOIN @inputTable2 t2 " +
				"ON t1.@idColumn1 = t2.@idColumn2";
		
		// Add time condition if dateColumn is present
		// The comparison "t2.@dateColumn <= t1.@baseDate" has to use <= to get the data from "the current date".
		// If you dislike it, use the "transition band".
		if (map.containsKey( "@dateColumn")) {
			if (bottomBounded) {
				pattern_code = pattern_code + 
						" WHERE DATE_ADD(t1.@baseDate, INTERVAL -1 YEAR) <= t2.@dateColumn AND t2.@dateColumn <= t1.@baseDate";
			} else {
				pattern_code = pattern_code + 
						" WHERE t2.@dateColumn <= t1.@baseDate";
			}
		}
				
		sql = addCreateTableAs(setting, pattern_code);
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, map.get("@outputTable"));
		sql = escapeEntityMap(setting, sql, map);
		
		// Drop old table (drop only once - do not drop freshly generated tables)
		// Network.executeUpdate(setting.connection, SQL.getDropTable(setting, map.get("@outputTable")));
					
		// Make the propagated table
		boolean result = Network.executeUpdate(setting.connection, sql);
		
		return result;
	}
	
	// 3a) Make a table with transmitted SQL statements. Useful for debugging. 
	// Note that this command and updates to the statement record are not logged for clarity reasons.
	public static boolean getStatementJournal(Setting setting) {
		String sql = "CREATE TABLE @outputTable ("+
						"statement_id int, " +
						"statement varchar(1024), " +
						"phase varchar(255), " +
						"timestamp timestamp NULL, " + 
						"runtime int, " + // Old MySQL and SQL92 do not have/require support for fractions of a second. 
						"error_code int, " +
						"error_message varchar(255), " +
						"PRIMARY KEY (statement_id))";
				
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, setting.statementTable);
		boolean result = Network.executeUpdate(setting.connection, sql);
		
		return result;
	} 
	
	// 4b) Add record into the statement journal
	// NEEDS FINISHING
	public static String addToStatementJournal(Setting setting, String statement, int errorCode, String errorMessage) {
			DateTimeFormatter formatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
			
			String sql = "INSERT INTO @outputTable VALUES (" +
		         "1, " +
		        "'" + statement + "', " +
		        "'" + "" + "', " +
		        "'" + LocalDateTime.now().format(formatter) + "', " +
		        "'" + 1 + "', " + 		
		        "'" + errorCode + "', " + 					// Should be a list...
		        "'" + errorMessage + "')";
		    
			sql = expandName(setting, sql);
			sql = escapeEntity(setting, sql, setting.statementTable);

			return sql;
		}
	
	// 4a) Return create journal_predictor table command
	public static String getJournal(Setting setting) {
		String sql = "CREATE TABLE @outputTable ("+
	      "predictor_id bigint, " +
	      "timestamp_start timestamp DEFAULT CURRENT_TIMESTAMP, " +
	      "timestamp_finish timestamp, " +  // Old MySQL and SQL92 do not have/require support for fractions of a second. 
	      "name varchar(255), " +
	      "table_list varchar(1024), " +
	      "column_list varchar(1024), " +
	      "propagation_path varchar(1024), " +	      
	      "date_constrain varchar(255), " +
	      "parameter_list varchar(1024), " +
	      "pattern_name varchar(254), " + 
	      "pattern_author varchar(254), " + 
	      "pattern_code varchar(1024), " +
	      "sql_code varchar(1024), " +
	      "target varchar(255), " +
	      "relevance decimal(18,10), " +
	      "qc_rowCount bigint DEFAULT '0', " +
	      "qc_nullCount bigint DEFAULT '0', " +
	      "is_ok smallint DEFAULT '0', " +
	      "PRIMARY KEY (predictor_id))";
		
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, setting.journalTable);

		return sql;
	}
	 
	// 4b) Add record into the journal_predictor
	public static String addToJournal(Setting setting, Predictor predictor) {
		DateTimeFormatter formatter =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
		
		String sql = "INSERT INTO @outputTable VALUES (" +
	        predictor.getId() + ", " +
	        "'" + predictor.getTimestampDesigned().format(formatter) + "', " +
	        "'" + predictor.getTimestampDelivered().format(formatter) + "', " +
	        "'" + predictor.getName() + "', " +
	        "'" + predictor.inputTableOriginal + "', " + 			
	        "'" + predictor.columnMap.toString() + "', " + 		// Should be a list...
	        "'" + predictor.propagationPath.toString() + "', " + 		   
	        "'" + predictor.propagationDate + "', " + 
	        "'" + predictor.getParameterList().toString() + "', " +  // Violates the 1st norm...
	        "'" + predictor.getPatternName() + "', " + 
	        "'" + predictor.patternAuthor + "', " + 
	        "'" + predictor.getPatternCode() + "', " +
	        "'" + predictor.getSql() + "', " +
			"'" + 0 + "', " + // Place holder for target
			"'" + 0 + "', " + // Place holder for Chi2
	        "'" + predictor.getRowCount() + "', " + 
	        "'" + predictor.getNullCount() + "', " + 
	        predictor.isOk() + ")";
	    
		sql = expandName(setting, sql);
		sql = escapeEntity(setting, sql, setting.journalTable);

		return sql;
	}
	
	// 5) Get predictor
	public static String getPredictor(Setting setting, Predictor predictor){
		String sql;
		
		String pattern_code = addCreateTableAs(setting, predictor.getPatternCode());
		pattern_code = expandName(setting, pattern_code);
		pattern_code = escapeEntity(setting, pattern_code, predictor.outputTable);
		sql = escapeEntityPredictor(setting, pattern_code, predictor);
		
		return sql;
	}
	
	// 6) Assembly the final step - the output table
	// THE CURRENT IMPLEMENTATION SCALES ONLY TO ~3600 PREDICTORS. BUT THAT IS SO FAR ACCEPTABLE AS A TABLE IN
	// A DATABASE IS COMMONLY LIMITED TO ~1600 COLUMNS. 
	public static void getMainSample(Setting setting, List<String> tableListAll, List<String> columnListAll) {
		// MySQL can join maximally 61 tables in a single command. Hence split the task into several smaller joins.
		List<List<String>> tableListSmall = Lists.partition(tableListAll, 60);
		List<List<String>> columnListSmall = Lists.partition(columnListAll, 60);
		
		// Prepare a list of the temporary table names
		List<String> tempTableList = new ArrayList<String>();
		
		// Create temporary tables
		for (int i = 0; i < tableListSmall.size(); i++) {
			// Initialization
			StringBuffer stringBuffer = new StringBuffer(500);
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
			pattern_code = expandName(setting, pattern_code);
			pattern_code = expandNameList(setting, pattern_code, tableList);
			pattern_code = escapeEntity(setting, pattern_code, tempTable); 
			
			// Escape table & column entities (tables can't be escaped in definition because they have to be first expanded...)
			Map<String, String> map = new HashMap<String, String>(61);
			for (String table : tableList) map.put(table, table);			// This is dangerous (no prefix... in the substitution) 
			for (String column : columnList) map.put("@" + column, column);
			String sql = escapeEntityMap(setting, pattern_code, map);
			
			// Execute the query
			Network.executeUpdate(setting.connection, sql);
			
			// Build index on BaseId in the temporary table for possibly faster final join
			Network.executeUpdate(setting.connection, getIndex(setting, tempTable));
			
		}
		
		//// Combine the temporary tables into a single output table ////
		StringBuffer stringBuffer = new StringBuffer(500);
		
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
		pattern_code = expandName(setting, pattern_code);
		pattern_code = expandNameList(setting, pattern_code, tempTableList);
		pattern_code = escapeEntity(setting, pattern_code, setting.sampleTable); 
		
		// Escape table & column entities (tables can't be escaped in definition because they have to be first expanded...)
		Map<String, String> map = new HashMap<String, String>(61);
		for (String table : tempTableList) map.put(table, table);
		for (String column : columnListAll) map.put("@" + column, column);
		String sql = escapeEntityMap(setting, pattern_code, map);
		
		// Execute the query
		Network.executeUpdate(setting.connection, sql);
	} 
}

	