package metaInformation;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import connection.Network;
import connection.SQL;
import run.Setting;
import utility.Meta;
import utility.Meta.Table;



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
		getDataTypes(setting);
		
		
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
			tableData.relationship = collectRelationships(setting, database, schema, tableName);
			
			// Store idColumn (as a side effect) and get a map of columns without ids
			columnMap = filterId(tableData, columnMap, tableData.relationship);
			
			// Deal with PKs
			String pk = getPrimaryKey(setting, database, schema, tableName);
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
	
	
	
	// 1) Get all relationships in the table. The returned list contains all {FTable, Column, FColumn} for the selected Table.
	private static List<List<String>> collectRelationships(Setting setting, String database, String schema, String table) {
		// Deal with catalog/schema less databases		
		// MySQL
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			database = schema;
			schema = null;
		}
		
		// SAS driver doesn't return keys. Use own query.
		if ("SAS".equals(setting.databaseVendor)) {
			return collectRelationshipsSAS(setting, schema, table);
		}
		
		
		// Initialization
		List<List<String>> relationshipSet = new ArrayList<List<String>>();
		
		// Get all relations coming from this table
		try {
			DatabaseMetaData meta = setting.connection.getMetaData();
			ResultSet rs = meta.getImportedKeys(database, schema, table);

			while (rs.next()) {
				ArrayList<String> relationship = new ArrayList<String>();
				relationship.add(rs.getString("PKTABLE_NAME"));
				relationship.add(rs.getString("FKCOLUMN_NAME"));
				relationship.add(rs.getString("PKCOLUMN_NAME"));
				relationshipSet.add(relationship);
			}
			
			// And now Exported keys
			rs = meta.getExportedKeys(database, schema, table);

			while (rs.next()) {
				ArrayList<String> relationship = new ArrayList<String>();
				relationship.add(rs.getString("FKTABLE_NAME"));
				relationship.add(rs.getString("PKCOLUMN_NAME"));
				relationship.add(rs.getString("FKCOLUMN_NAME"));
				relationshipSet.add(relationship);
			}
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		
		// Output Quality Control
		if (relationshipSet.isEmpty()) {
			logger.info("Table " + table + " doesn't have any predefined relationship.");
		}
		
		return relationshipSet;
	}

	
	// SAS JDBC driver doesn't return keys. Use dictionary tables instead.
	// See: www2.sas.com/proceedings/sugi30/070-30.pdf
	private static List<List<String>> collectRelationshipsSAS(Setting setting, String schema, String table) {
		// Initialization
		List<List<String>> relationshipSet = new ArrayList<List<String>>();
		String sql = "select t1.memname as FKTABLE_NAME " +
							", t1.unique_memname as PKTABLE_NAME " +
							", t2.column_name as FKCOLUMN_NAME " +
							", t3.column_name as PKCOLUMN_NAME " +
						"from dictionary.REFERENTIAL_CONSTRAINTS t1 " +
						"join dictionary.CONSTRAINT_COLUMN_USAGE t2 " +
						"on t1.libname = t2.table_catalog " +
						"and t1.memname = t2.table_name " +
						"and t1.constraint_name = t2.constraint_name " +
						"join dictionary.CONSTRAINT_COLUMN_USAGE t3 " +
						"on t1.unique_libname = t3.table_catalog " +
						"and t1.unique_memname = t3.table_name " +
						"and t1.unique_constraint_name = t3.constraint_name ";
						
		
		// Get all relations coming from this table
		try (Statement stmt = setting.connection.createStatement()) {

			String condition = "where t1.libname = '" + schema + "' and t1.memname = '" + table + "'";
			ResultSet rs = stmt.executeQuery(sql + condition);

			while (rs.next()) {
				ArrayList<String> relationship = new ArrayList<String>();
				relationship.add(rs.getString("PKTABLE_NAME").replace(" ", ""));
				relationship.add(rs.getString("FKCOLUMN_NAME").replace(" ", ""));
				relationship.add(rs.getString("PKCOLUMN_NAME").replace(" ", ""));
				relationshipSet.add(relationship);
			}

			// And now Exported keys
			condition = "where t1.unique_libname = '" + schema + "' and t1.unique_memname = '" + table + "'";
			rs = stmt.executeQuery(sql + condition);

			while (rs.next()) {
				ArrayList<String> relationship = new ArrayList<String>();
				relationship.add(rs.getString("FKTABLE_NAME").replace(" ", ""));
				relationship.add(rs.getString("PKCOLUMN_NAME").replace(" ", ""));
				relationship.add(rs.getString("FKCOLUMN_NAME").replace(" ", ""));
				relationshipSet.add(relationship);
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		return relationshipSet;
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
	
	// 1.5) Get the single primary key (It would be the best if only artificial keys were returned. At least 
	// we are excluding the composite keys).
	private static String getPrimaryKey(Setting setting, String database, String schema, String table) {
		// If the database doesn't support schemas, use schema name as database name (java treats
		// schema-less databases differently than SQL databases do)
		if (!setting.supportsCatalogs) {
			database = schema;
			schema = null;
		}
		
		// Initialization
		List<String> primaryKeyList = new ArrayList<String>();
		
		// Get all columns making the primary key
		try {
			DatabaseMetaData meta = setting.connection.getMetaData();
			ResultSet rs = meta.getPrimaryKeys(database, schema, table);

			while (rs.next()) {
				primaryKeyList.add(rs.getString("COLUMN_NAME"));
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
		
		// If the table contains a PK composed of exactly one column, return the name of the column 
		if (primaryKeyList.size() == 1) {
			return primaryKeyList.get(0);
		}
		
		// Otherwise return null;
		return null;
	}
	
	// 2) Store idColumn and return column map without ids
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
	
	// 3) Get list of supported data types
	private static void getDataTypes(Setting setting) {
		
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
			
		// Log the result
		logger.debug("Supported data types are: " + dataTypeList.toString());	
	}
	
	


}
