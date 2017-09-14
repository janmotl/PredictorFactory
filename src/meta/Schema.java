package meta;

import org.apache.log4j.Logger;
import run.Setting;
import utility.BlackWhiteList;
import utility.Meta;
import utility.TextParser;

import java.util.*;


public class Schema {

	// Logging
	private static final Logger logger = Logger.getLogger(Schema.class.getName());

	private String name;                                     // To make Schema self-sufficient even outside the map
	private SortedMap<String, Table> tableMap = new TreeMap<>(); // tableName --> Table

	// Constructors
	public Schema(String name, SortedMap<String, Table> tableMap) {
		this.name = name;
		this.tableMap = Collections.unmodifiableSortedMap(tableMap);
	}

	public Schema(Setting setting, String schemaName) {
		name = schemaName;

        // Get the relationships from the DDL and XML files for whole schema -> we parse the files just once
		List<ForeignConstraint> external = ForeignConstraintDDL.unmarshall("foreignConstraint.externalFCs", setting.targetSchema);
        external.addAll(ForeignConstraintList.unmarshall("foreignConstraint.xml").foreignConstraint);
        external = Meta.addReverseDirections(external); // Make the FCs bidirectional

        // If it is a target schema, we perform additional checks
		if (setting.targetSchema.equals(schemaName)) {
			tableMap = getTables(setting, schemaName, external);
		} else {
			tableMap = getTablesBasic(setting, schemaName, external);
		}
	}

	// Convenience getters
	public Table getTable(String tableName) {
		return tableMap.get(tableName);
	}

	public Collection<Table> getAllTables() {
		return tableMap.values();
	}

	// Note: These methods should be private -> rewrite the tests to use the constructor.
	public static SortedMap<String, Table> getTablesBasic(Setting setting, String schema, List<ForeignConstraint> externalFCs) {

		// Initialization
		SortedMap<String, Table> tableMap;
		Map<String, List<String>> whiteListTable = TextParser.list2map(TextParser.string2list(setting.whiteListTable), setting.targetSchema);
		Map<String, List<String>> blackListTable = TextParser.list2map(TextParser.string2list(setting.blackListTable), setting.targetSchema);
		Map<String, Map<String, List<String>>> whiteMapColumn = TextParser.list2mapMap(TextParser.string2list(setting.whiteListColumn), setting.targetSchema);
		Map<String, Map<String, List<String>>> blackMapColumn = TextParser.list2mapMap(TextParser.string2list(setting.blackListColumn), setting.targetSchema);


		// Get tables
		tableMap = Meta.collectTables(setting, setting.database, schema);

		// Apply black/white lists
		tableMap = BlackWhiteList.filter(tableMap, blackListTable.get(schema), whiteListTable.get(schema));

		// QC that count of tables after blacklisting is > 0
		if (tableMap.isEmpty()) {
			logger.warn("The count of permitted tables is 0. Check your blacklist/whitelist.");
		}

		// Collect columns and relationships
		whiteMapColumn.putIfAbsent(schema, new TreeMap<>());
		blackMapColumn.putIfAbsent(schema, new TreeMap<>());
		getColumns(setting, schema, tableMap, whiteMapColumn.get(schema), blackMapColumn.get(schema), externalFCs);

		// QC that relationships were extracted
		int relationshipCount = 0;
		for (Table table : tableMap.values()) {
			relationshipCount += table.foreignConstraintList.size();
		}
		if (relationshipCount == 0) {
			logger.warn("No relationships were detected. You may either define Foreign Key Constrains in the database. Or you may create and put foreignConstraint.xml into config directory (note: entity names in the xml must exactly match the entity names in the database, EVEN if the database is case insensitive, because comparisons are performed locally, not at the server).");
		}

		return tableMap;
	}

	// Return map of tables in the schema
	// IDS ARE WRONGFULLY IGNORED FROM THE BLACK/WHITE LISTS
	// Note: We parse the content of black/white-lists repeatably. That's not nice.
	// Note: Merge getTables and getTablesBasic together - who has to maintain them both?! Use ifs to skip unrelated calls.
	public static SortedMap<String, Table> getTables(Setting setting, String schema, List<ForeignConstraint> externalFCs) {

		// Initialization
		SortedMap<String, Table> tableMap;
		Map<String, List<String>> whiteListTable = TextParser.list2map(TextParser.string2list(setting.whiteListTable), setting.targetSchema);
		Map<String, List<String>> blackListTable = TextParser.list2map(TextParser.string2list(setting.blackListTable), setting.targetSchema);
		Map<String, Map<String, List<String>>> whiteMapColumn = TextParser.list2mapMap(TextParser.string2list(setting.whiteListColumn), setting.targetSchema);
		Map<String, Map<String, List<String>>> blackMapColumn = TextParser.list2mapMap(TextParser.string2list(setting.blackListColumn), setting.targetSchema);


		// Get tables
		tableMap = Meta.collectTables(setting, setting.database, schema);

		// QC the tables
		qcTablesBeforeBlacklisting(setting, tableMap);

		// Apply black/white lists
		tableMap = BlackWhiteList.filter(tableMap, blackListTable.get(schema), whiteListTable.get(schema));

		// QC the tables
		qcTablesAfterBlacklisting(setting, tableMap);


		// Collect columns and relationships
		whiteMapColumn.putIfAbsent(schema, new TreeMap<>());
		blackMapColumn.putIfAbsent(schema, new TreeMap<>());
		getColumns(setting, schema, tableMap, whiteMapColumn.get(schema), blackMapColumn.get(schema), externalFCs);

		// QC the columns and relationships
		qcColumns(setting, tableMap);

		// Do not use targetColumn as an input attribute if we work with a static dataset.
		// But use the targetColumn in dynamic datasets.
		if (setting.targetDate == null) {
			for (String targetColumn : setting.targetColumnList) {
				tableMap.get(setting.targetTable).getColumn(targetColumn).isNominal = false;
				tableMap.get(setting.targetTable).getColumn(targetColumn).isNumerical = false;
				tableMap.get(setting.targetTable).getColumn(targetColumn).isTemporal = false;
			}
		}

		// Get the pivot date for targetDate
		// Note: It is ugly we are modifying the "setting" here. Neither MetaInput is good because in the time we
		// need pivotDate we are already using MetaOutput. And MetaOutput is concerned with table-level and column-level
		// info. Maybe we could move this out. But the only good current place is Launcher...
		// Maybe this information should be encoded in @baseFold?
		if (setting.targetDate != null) {
			int dateDataType = tableMap.get(setting.targetTable).getColumn(setting.targetDate).dataType;
			setting.pivotDate = setting.dialect.getPivotDate(setting, dateDataType);
		}

		// Copy target column values into the Setting
		// Note: We do it only because it is difficult to get it from metaInput -> make it easier (we should directly call
		// the method in the Column class).
		setting.targetUniqueValueMap = getUniqueTargetValueMap(setting, tableMap);

		return tableMap;
	}


	//////// Subroutines ////////


	private static void qcTablesBeforeBlacklisting(Setting setting, SortedMap<String, Table> tableMap) {
		// QC that targetTable is in the tableSet
		if (!tableMap.keySet().contains(setting.targetTable)) {
			logger.warn("The target table '" + setting.targetTable + "' doesn't exist in the database.");
			logger.warn("Available tables in the database are: " + tableMap.keySet());
		}

		// QC that count of tables is > 0
		if (tableMap.isEmpty()) {
			logger.warn("The count of available tables is 0.");
		}
	}

	private static void qcTablesAfterBlacklisting(Setting setting, SortedMap<String, Table> tableMap) {
		// QC that count of tables after blacklisting is > 0
		if (tableMap.isEmpty()) {
			logger.warn("The count of permitted tables is 0. Check your blacklist/whitelist.");
		}

		// QC that targetTable exists and we can use it
		if (!tableMap.containsKey(setting.targetTable)) {
			logger.warn("The target table " + setting.targetTable +" is not among the permitted tables. Check your blacklist/whitelist.");
		}
	}

	private static void getColumns(Setting setting, String schema, SortedMap<String, Table> tableMap, Map<String, List<String>> whiteMapColumn, Map<String, List<String>> blackMapColumn, List<ForeignConstraint> externalFCs) {
		// I could have collected all the metadata at schema level. The runtime would be faster,
		// because just one query would be performed (instead of making a unique query for each table).
		// But implementation would be slightly more complex.
		for (Table table : tableMap.values()) {

			// Collect columns
			table.columnMap = Meta.collectColumns(setting, setting.database, schema, table.name);

			// Respect white/black lists
			table.columnMap = BlackWhiteList.filter(table.columnMap, blackMapColumn.get(table.name), whiteMapColumn.get(table.name));

			// Store relationships from the database, DDL and XML
			table.foreignConstraintList = Meta.collectRelationships(setting, schema, table.name);
            table.foreignConstraintList.addAll(Meta.getTableForeignConstraints(externalFCs, table.name));


			// Identify ids based on FK
			table.identifyId();

			// Identify artificial ids based on PK
			String pk = Meta.getPrimaryKey(setting, setting.database, schema, table.name);
			if (pk != null && table.columnMap.containsKey(pk)) {
				table.getColumn(pk).isId = true;
				table.getColumn(pk).isUnique = true;
			}

			// Unique constraints
			List<String> uniques = Meta.getUniqueColumns(setting, setting.database, schema, table.name);
			for (String column : uniques) {
				if (table.columnMap.containsKey(column)) {  // Some of the columns can be blacklisted -> we have to validate the presence
					table.getColumn(column).isUnique = true;
				}
			}

			// Store numericalColumn, nominalColumn and temporalColumn
			table.categorizeColumns(setting);
		}
	}

	private static void qcColumns(Setting setting, SortedMap<String, Table> tableMap) {
		// QC that all target columns are present
		for (String targetColumn : setting.targetColumnList) {
			if (!tableMap.get(setting.targetTable).columnMap.containsKey(targetColumn)) {
				logger.warn("Target column '" + targetColumn + "' is not among the permitted columns. Check your blacklist/whitelist.");
			}
		}

		// QC that all target columns are numerical if performing regression.
		if ("regression".equals(setting.task)) {
			for (String targetColumn : setting.targetColumnList) {
				Column column = tableMap.get(setting.targetTable).getColumn(targetColumn);
				if (!column.isNumerical) {
					logger.warn("Target column '" + targetColumn + "' is " + column.dataTypeName + ". But a numerical data type was expected since regression task is set.");
				}
			}
		}

		// QC that all target IDs are present
		for (String idColumn : setting.targetIdList) {
			if (!tableMap.get(setting.targetTable).columnMap.containsKey(idColumn)) {
				logger.warn("Id column '" + idColumn + "' is not among the permitted columns. Check your blacklist/whitelist.");
			}
		}

		// QC that target IDs do not contain nulls
		for (String targetId : setting.targetIdList) {
			Column column = tableMap.get(setting.targetTable).getColumn(targetId);
			if (column.containsNull(setting)) {
				logger.warn("Target id column '" + column.name + "' contains null. Are you sure you have selected correct target ids?");
			}
		}


		// QC that targetDate doesn't contain nulls
		if (setting.targetDate != null) {
			Column column = tableMap.get(setting.targetTable).getColumn(setting.targetDate);
			if (column.containsNull(setting)) {
				logger.warn("Target date column '" + setting.targetDate + "' contains null. Rows with the null in target date column WILL BE IGNORED!");
			}
		}

		// QC that TargetDate is temporal
		if (setting.targetDate != null) {
			Column column = tableMap.get(setting.targetTable).getColumn(setting.targetDate);
			if (!column.isTemporal) {
				logger.warn("Target date column '" + setting.targetDate + "' is " + column.dataTypeName + ". But a temporal data type was expected.");
			}
		}

		// QC that relationships were extracted
		int relationshipCount = 0;
		for (Table table : tableMap.values()) {
			relationshipCount += table.foreignConstraintList.size();
		}
		if (relationshipCount == 0) {
			logger.warn("No relationships were detected. You may either define Foreign Key Constrains in the database. Or you may create and put foreignConstraint.xml into config directory (note: entity names in the xml must exactly match the entity names in the database, EVEN if the database is case insensitive, because comparisons are performed locally in Java, not at the server).");
		}
	}


//  public static boolean isSymmetrical(){
//      for (String table : tableSet) {
//          // Get relationships
//          List<List<String>> relationshipList = MetaInput.downloadRelationships(setting, setting.database, setting.inputSchemaList, table);
//          System.out.println(relationshipList);
//
//          // Detection
//          String lagFTable = "";
//          String lagColumn = "";
//          for (List<String> relationship : relationshipList) {
//              if (relationship.get(0).equals(lagFTable) && relationship.get(1).equals(lagColumn)) {
//                  System.out.println("bingo " + lagFTable);
//              }
//              lagFTable = relationship.get(0);
//              lagColumn = relationship.get(1);
//          }
//      }
//  }



	// Return sets of unique values for the target columns
	// The returned map contains: targetColumn -> set(values)
	private static Map<String, Set<String>> getUniqueTargetValueMap(Setting setting, SortedMap<String, Table> tableMap) {
		Map<String, Set<String>> uniqueValueMap = new HashMap<>();
		Table targetTable = tableMap.get(setting.targetTable);

		for (String targetColumn : setting.targetColumnList) {
			uniqueValueMap.put(targetColumn, targetTable.getColumn(targetColumn).getUniqueValues(setting));
		}

		return uniqueValueMap;
	}
}