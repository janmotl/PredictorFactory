package metaInformation;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import run.Setting;
import utility.Meta;
import utility.Meta.Table;
import connection.SQL;



public class MetaInput {
	
	// Logging
	public static final Logger logger = Logger.getLogger(MetaInput.class.getName());


	// Return map of tables in the input schema
	public static SortedMap<String, Table> getMetaInput(Setting setting) {
		
		// Initialization
		SortedMap<String, Table> tableMap = new TreeMap<String, Table>(); // Map of {tableName, tableData}
		String database = setting.database;
		String schema = setting.inputSchema;

		
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
		
		// Validate that targetTable is in the tableSet
		if (!tableSet.contains(setting.targetTable)) {
			logger.warn("The target table '" + setting.targetTable + "' doesn't exist in the database.");
			logger.warn("Available tables are: " + tableSet);
		}
		
		// Respect table blacklist
		if (setting.blackListTable != null) {
			List<String> blackListTable = Arrays.asList(setting.blackListTable.split(","));
			tableSet.removeAll(blackListTable);
		}
		
		// Initialize column blacklist
		List<String> blackListColumn = new ArrayList<String>();
		if (setting.blackListColumn != null) {
			blackListColumn = Arrays.asList(setting.blackListColumn.split(","));
		}
		
		// Initialize table-map with Table objects 
		tableMap.clear(); // We have to remove tables from previous runs 
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
			
			// Respect column blacklist
			for (String blackTuple : blackListColumn) {
				List<String> blackTableColumn = Arrays.asList(blackTuple.split("\\.")); // Dot, but escaped for regex
				if (blackTableColumn.get(0).equals(tableName)) {
					columnMap.remove(blackTableColumn.get(1));
				}
			}
						
			// Store relationships
			Table tableData = tableMap.get(tableName);
			tableData.relationship = Meta.collectRelationships(setting, database, schema, tableName);
			
			// Store idColumn (as a side effect) and get a map of columns without ids
			columnMap = filterId(tableData, columnMap, tableData.relationship);
			
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
			for (String columnName : tableData.nominalColumn) {
				tableData.uniqueList.put(columnName, SQL.getUniqueRecords(setting, tableName, columnName, true));
			}		
		}
		
		// Get distinct values for the target iff we are performing classification AND target is a numerical column
		if ("classification".equals(setting.task) && tableMap.get(setting.targetTable).numericalColumn.contains(setting.targetColumn)) {
			tableMap.get(setting.targetTable).uniqueList.put(setting.targetColumn, SQL.getUniqueRecords(setting, setting.targetTable, setting.targetColumn, true));
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
		
		// Output quality control
		int relationshipCount = 0;
		for (Table table : tableMap.values()) {
			relationshipCount += table.relationship.size();
		}
		if (relationshipCount == 0) {
			logger.warn("No relationships were detected");
		}
		
		return tableMap;
	}
	
	
	
//	public static boolean isSymmetrical(){
//		for (String table : tableSet) {
//			// Get relationships 
//			List<List<String>> relationshipList = MetaInput.collectRelationships(setting, setting.database, setting.inputSchema, table);
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
	private static Map<String, Integer> filterId(Table table, Map<String, Integer> columnMap, List<List<String>> relationship) {
		
		// Initialization
		SortedSet<String> idColumnSet = new TreeSet<String>();
		
		// Add ids present in FK constrains 
		for (List<String> relation : relationship) {
			String idColumnName = relation.get(1);	// Get the id column name
			columnMap.remove(idColumnName);			// Remove the id column name
			idColumnSet.add(idColumnName);			// And put the id column name into a special set
		}
				
		// Store the id set
		table.idColumn = idColumnSet;
		
		return columnMap;
	}
	
	// 2) Get list of supported data types
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
	
}
