package metaInformation;

import connection.SQL;
import org.apache.log4j.Logger;
import run.Setting;
import utility.Meta;
import utility.Meta.Table;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


public class MetaInput {
	
	// Logging
	private static final Logger logger = Logger.getLogger(MetaInput.class.getName());


	// Return map of tables in the input schema
	// IDS ARE WRONGFULLY IGNORED FROM THE BLACK/WHITE LISTS
	public static SortedMap<String, Table> getMetaInput(Setting setting) {
		
		// Initialization
		SortedMap<String, Table> tableMap = new TreeMap<>(); // Map of {tableName, tableData}
		final String database = setting.database;
		final String schema = setting.inputSchema;
		final List<String> whiteListTable = string2list(setting.whiteListTable); // Parsed values
		final List<String> blackListTable = string2list(setting.blackListTable); // Parsed values
		final Map<String,List<String>> whiteMapColumn = list2map(string2list(setting.whiteListColumn)); // Parsed values
		final Map<String,List<String>> blackMapColumn = list2map(string2list(setting.blackListColumn)); // Parsed values
		
		// What data types are used by the database? In journal file we use {3, 4, 12 and 93}.
		logger.debug("Supported data types are: " + getDataTypes(setting)); 

		// QC input and output schemas
		SortedSet<String> schemaSet = Meta.collectSchemas(setting, setting.database);
		
		if (setting.inputSchema != null && !schemaSet.contains(setting.inputSchema)) {
			logger.warn("The input schema '" + setting.inputSchema + "' doesn't exist in the database.");
			logger.warn("Available schemas in the database are: " + schemaSet);
		}
		if (setting.outputSchema != null && !schemaSet.contains(setting.outputSchema)) {
			logger.warn("The output schema '" + setting.outputSchema + "' doesn't exist in the database.");
			logger.warn("Available schemas in the database are: " + schemaSet);
		}
		
				
		// Get tables
		SortedSet<String> tableSet = utility.Meta.collectTables(setting, database, schema);
		
		// QC that targetTable is in the tableSet
		if (!tableSet.contains(setting.targetTable)) {
			logger.warn("The target table '" + setting.targetTable + "' doesn't exist in the database.");
			logger.warn("Available tables in the database are: " + tableSet);
		}

		// QC that targetDate doesn't contain nulls
//		if (setting.targetDate != null) {
//			if ((SQL.getRowCount(setting, setting.inputSchema, setting.targetTable) - SQL.getNotNullCount(setting, setting.inputSchema, setting.targetTable, setting.targetDate)) > 0) {
//				logger.warn("Target date column '" + setting.targetDate + "' contains null. Rows with the null in target date column WILL BE IGNORED!");
//			}
//		}

		// Apply black/white lists
		if (!whiteListTable.isEmpty()) {
			tableSet.retainAll(whiteListTable);	// If whiteList is used, perform intersect with the available tables
		}
		tableSet.removeAll(blackListTable); // Remove blackListed tables
		
		// QC: Count of tables after blacklisting should be > 0
		if (tableSet.isEmpty()) {
			logger.warn("The count of available tables is 0.");
		}


		
		// Initialize table-map with Table objects 
		tableMap.clear(); // We have to remove tables from previous runs (WTF?)
		for (String tableName : tableSet) {
			Table tableData = new Table();
			tableMap.put(tableName, tableData);
		}
					
		// Collect columns and relationships
		// I could have collected all the metadata at schema level. The runtime would be faster,
		// because just one query would be performed (instead of making a unique query for each table).
		// But implementation would be slightly more complex.
		for (String tableName : tableMap.keySet()) {
			
			// Collect columns
			Map<String, Integer> columnMap = Meta.collectColumns(setting, database, schema, tableName);

			// Respect white/black lists
			if (whiteMapColumn.get(tableName) != null) {
				columnMap.keySet().retainAll(whiteMapColumn.get(tableName)); // If applicable, calculate intersect
			}
			if (blackMapColumn.get(tableName) != null) {
				columnMap.keySet().removeAll(blackMapColumn.get(tableName)); // If applicable, remove all blacklisted columns
			}

			// Store relationships
			Table tableData = tableMap.get(tableName);
			tableData.foreignConstraintList = Meta.collectRelationships(setting, schema, tableName);
			
			// Store idColumn (as a side effect) and get a map of columns without ids
			columnMap = filterId(tableData, columnMap, tableData.foreignConstraintList);
			
			// Deal with PKs
			String pk = Meta.getPrimaryKey(setting, database, schema, tableName);
			if (pk!=null) {
				columnMap.remove(pk);
				tableData.idColumn.add(pk);
			}
			
			// Store numericalColumn, nominalColumn and timeColumn
			tableData = Meta.categorizeColumns(tableData, columnMap, tableName);
			
			// If we are performing classification, set target column to nominal.
			// This is useful, since integer column can be used to signal the class.
			// The minimal purpose: we want to get unique values in the target column.
			if ("classification".equals(setting.task) && setting.targetTable.equals(tableName)) {
				tableData.nominalColumn.add(setting.targetColumn);
				tableData.numericalColumn.remove(setting.targetColumn);
			}

			// Get distinct values for each nominal column. 
			// The unique values will be used in patterns like "WoE" or "Existential count".
//			for (String columnName : tableData.nominalColumn) {
//				tableData.uniqueList.put(columnName, SQL.getUniqueRecords(setting, tableName, columnName, true));
//			}		
		}
		
		// Get distinct values for the target iff we are performing classification AND target is a numerical column
		// (in the case the target is nominal we already have the unique values)
		if ("classification".equals(setting.task) && tableMap.get(setting.targetTable).numericalColumn.contains(setting.targetColumn)) {
			tableMap.get(setting.targetTable).uniqueList.put(setting.targetColumn, SQL.getUniqueRecords(setting, setting.targetTable, setting.targetColumn, true));
		}
		
		// If the target is nominal, store the information.
		if (tableMap.get(setting.targetTable).nominalColumn.contains(setting.targetColumn)) {
			setting.isTargetNominal = true;
		}
		
		// Do not use targetColumn as a predictor. 
		// HOWEVER, TARGET COLUMN SHOULD BE CONSIDERED IF DATE COLUMN IS USED!
		if (tableMap.containsKey(setting.targetTable)) {
			tableMap.get(setting.targetTable).nominalColumn.remove(setting.targetColumn);	
			tableMap.get(setting.targetTable).numericalColumn.remove(setting.targetColumn);
			tableMap.get(setting.targetTable).timeColumn.remove(setting.targetColumn);
		} else {
			logger.warn("The target table was not found among the traversed tables. Check your blacklist/whitelist.");
		}
		
		// QC that relationships were extracted
		int relationshipCount = 0;
		for (Table table : tableMap.values()) {
			relationshipCount += table.foreignConstraintList.size();
		}
		if (relationshipCount == 0) {
			logger.warn("No relationships were detected");
		}
		
		return tableMap;
	}
	
	
	
//	public static boolean isSymmetrical(){
//		for (String table : tableSet) {
//			// Get relationships 
//			List<List<String>> relationshipList = MetaInput.downloadRelationships(setting, setting.database, setting.inputSchema, table);
//			System.out.println(relationshipList);
//			
//			// Detection
//			String lagFTable = "";
//			String lagColumn = "";
//			for (List<String> relationship : relationshipList) {
//				if (relationship.get(0).equals(lagFTable) && relationship.get(1).equals(lagColumn)) {
//					System.out.println("bingo " + lagFTable);
//				}
//				lagFTable = relationship.get(0);
//				lagColumn = relationship.get(1);
//			}
//		}
//	}
	

	// 1) Store idColumn and return column map without ids
	private static Map<String, Integer> filterId(Table table, Map<String, Integer> columnMap, List<ForeignConstraint> relationship) {
		
		// Initialization
		SortedSet<String> idColumnSet = new TreeSet<String>();
		
		// Add ids present in FK constrains 
		for (ForeignConstraint relation : relationship) {
			for (String idColumnName : relation.column) {
				columnMap.remove(idColumnName);			// Remove the id column name
				idColumnSet.add(idColumnName);			// And put the id column name into a special set
			}
		}
				
		// Store the id set
		table.idColumn = idColumnSet;
		
		return columnMap;
	}
	
	// 2) Get list of supported data types from the database
	private static List<List<String>> getDataTypes(Setting setting) {
		
		// Initialization
		List<List<String>> dataTypeList = new ArrayList<List<String>>();
		
		// Get all relations from this table
		try {
			DatabaseMetaData meta = setting.connection.getMetaData();
			ResultSet rs = meta.getTypeInfo();

			while (rs.next()) {
				ArrayList<String> dataType = new ArrayList<String>();
				dataType.add(rs.getString("TYPE_NAME"));
				dataType.add(rs.getString("DATA_TYPE"));
				dataTypeList.add(dataType);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
			
		return dataTypeList;	
	}

	// 3) Convert comma delimited string to list
	private static List<String> string2list(String string) {
		// Initialization
		List<String> result = new ArrayList<>();

		// Deal with nulls and empty strings
		if (string == null || string.isEmpty()) return result;

		// Parsing
		return Arrays.asList(string.split(","));
	}

	// 4) Convert dot delimited list to map
	private static Map<String, List<String>> list2map(List<String> list) {
		Map<String, List<String>> result = new HashMap<>();

		for (String tuple : list) {
			String[] tableColumn = tuple.split("\\."); // Dot, but escaped for regex
			if (tableColumn.length != 2) continue;	// Take care of empty strings...
			String table = tableColumn[0];
			String column = tableColumn[1];

			if (result.containsKey(table)) {
				result.get(table).add(column); // Add column into a present table
			} else {
				result.put(table, new LinkedList<>(Arrays.asList(column))); // Make a new table with the column
			}
		}

		return result;
	}


}
