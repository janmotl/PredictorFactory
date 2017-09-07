package meta;

import org.apache.log4j.Logger;
import run.Setting;
import utility.Meta;

import java.util.*;


public class Database {
	// Logging
	private static final Logger logger = Logger.getLogger(Database.class.getName());

	private SortedMap<String, Schema> schemaMap = new TreeMap<>();

	// Constructors
	public Database(SortedMap<String, Schema> schemaMap) {
		this.schemaMap = schemaMap;
	}

	public Database(Setting setting) {
		// QC the target, input & output schemas
		qcSchemas(setting);

		for (String schemaName : setting.inputSchemaList) {
			schemaMap.put(schemaName, new Schema(setting, schemaName));
		}
	}

	// Convenience getters
	public Table getTargetTable(Setting setting) {
		return schemaMap.get(setting.targetSchema).getTable(setting.targetTable);
	}

	public Schema getSchema(String schemaName) {
		return schemaMap.get(schemaName);
	}

	public List<Table> getAllTables() {
		List<Table> tables = new ArrayList<>();

		for (Schema schema : schemaMap.values()) {
			tables.addAll(schema.getAllTables());
		}

		return tables;
	}

	// Subroutines
	private static void qcSchemas(Setting setting) {
		SortedSet<String> schemaSet = Meta.collectSchemas(setting, setting.database);

		if (setting.targetSchema != null && !schemaSet.contains(setting.targetSchema)) {
			logger.warn("The target schema '" + setting.targetSchema + "' doesn't exist in the database.");
			logger.warn("Available schemas in the database are: " + schemaSet);
		}
		if (setting.outputSchema != null && !schemaSet.contains(setting.outputSchema)) {
			logger.warn("The output schema '" + setting.outputSchema + "' doesn't exist in the database.");
			logger.warn("Available schemas in the database are: " + schemaSet);
		}
		if (setting.inputSchemaList.size()<1){
			logger.warn("The input schema has to contain at least the target schema - it cannot be empty.");
		}
		for (String inputSchema : setting.inputSchemaList) {
			if (inputSchema != null && !schemaSet.contains(inputSchema)) {
				logger.warn("The input schema '" + inputSchema + "' doesn't exist in the database.");
				logger.warn("Available schemas in the database are: " + schemaSet);
			}
		}
	}


}
