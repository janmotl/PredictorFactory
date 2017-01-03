package meta;

import org.apache.log4j.Logger;
import run.Setting;

import java.util.*;


// Define struct.
// Always create collections to avoid null pointer exception and need to create collections at many places.
// TO MAKE LIVE NICER WE COULD MAKE THE MAPS PRIVATE AND IMPLEMENT FOLLOWING INTERFACE: {!!!get(columnName)!!!, add{column},
// addAll(Set<column>), retainAll(Set<columnName>), removeAll(Set<columnName>)}.
// WE SHOULD DO THE SAME FOR META: getTable(tableName)
public class Table {

	// Logging
	private static final Logger logger = Logger.getLogger(Table.class.getName());


	public String name;                                         // To make Table self-sufficient even outside the map
	public SortedMap<String, Column> columnMap = new TreeMap<>();   // All columns (but eventually blacklisted)
	public List<ForeignConstraint> foreignConstraintList = new ArrayList<>();   // NOTE: Should we create a single ForeignConstraintList?
	public boolean isTargetIdUnique;                                // Does combination {baseId, baseDate} repeat?


	// It makes sens to store columns in a single collection to avoid duplication.
	// These getters are not the most efficiently implemented. Feel free to replace it with a multi-key map.
	// Or we may keep private string lists for each type.
	// We are returning sets, hence, it is easy to get unique columns that are nominal OR numerical...
	public SortedSet<Column> getColumns(Setting setting, StatisticalType statisticalType) {
		SortedSet<Column> result = new TreeSet<>();

		for (Column column : columnMap.values()) {
			if (statisticalType == StatisticalType.ID && column.isId) result.add(column);
			if (statisticalType == StatisticalType.NOMINAL && column.isNominal) result.add(column);
			if (statisticalType == StatisticalType.NUMERICAL && column.isNumerical) result.add(column);
			if (statisticalType == StatisticalType.TEMPORAL && column.isTemporal) result.add(column);
		}

		// Use ids for feature calculation? Nevertheless, always allow targetColumn.
		if (!setting.useIdAttributes) {
			result.removeIf(column -> column.isId && !(setting.targetColumn.equals(column.name) && setting.targetTable.equals(name)));
		}

		return result;
	}

	// Return all columns that were not blacklisted
	public SortedSet<Column> getColumns() {
		return (SortedSet<Column>) columnMap.values();
	}

	// If the column is not in the map, raise an exception
	public Column getColumn(String columnName) {
		// Could extract the value and check for NULL for speed up (http://stackoverflow.com/questions/3626752/key-existence-check-in-hashmap)
		if (!columnMap.containsKey(columnName))
			throw new NullPointerException("The column " + columnName + " was not found in table " + name);
		return columnMap.get(columnName);
	}

	// Get the top N most frequent unique values for each nominal attribute.
	// However, if we are performing classification, uniqueList will contain all unique
	// values regardless of target's data type (can be String as well as Integer)
	// SHOULD BE EVALUATED LAZILY AND MEMOIZED
	public void addUniqueValues(Setting setting) {

		// Get setting.valueCount most frequent distinct values for each nominal column.
		// The unique values will be used in patterns like "WoE" or "Existential count".
		for (Column column : getColumns(setting, StatisticalType.NOMINAL)) {
			column.uniqueValueSet.addAll(setting.dialect.getTopUniqueRecords(setting, name, column.name));
		}

	}

	// Get numerical, nominal and time columns.
	public void categorizeColumns(Setting setting) {
		// Parameter control
		if (columnMap == null || columnMap.isEmpty()) {
			logger.info("Table: " + name + " doesn't contain any column beside ids.");
			return;
		}

		// Initialization
		int columnCounter = 0;

		// Categorize the dataTypes. If the data type is not recognized, the column is ignored.
		// This is intentional because we do not know how to deal with blobs (dataType = 2004)...
		// And we don't want them to slow down the mining process -> do not propagate them.
		// Doc: https://docs.oracle.com/cd/A97337_01/ias102_otn/buslog.102/bc4j/bc_abcdatatypes.htm
		// Doc: http://alvinalexander.com/java/edu/pj/jdbc/recipes/ResultSet-ColumnType.shtml
		// Doc: http://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
		for (Column column : columnMap.values()) {
			int dataType = column.dataType;

			if (dataType == -16 || dataType == -15 || dataType == -9 || dataType == -1 || dataType == 1 || dataType == 12) {
				column.isNominal = true;
			} else if ((dataType >= -7 && dataType <= -5) || (dataType >= 2 && dataType <= 8) || dataType == 16) {
				column.isNumerical = true;
			} else if (dataType == 91 || dataType == 92 || dataType == 93 || dataType == 2013 || dataType == 2014) {
				column.isTemporal = true;
			} else {
				columnCounter++;
				logger.debug("Ignoring column: " + name + "." + column.name
						+ " because it is of an unsupported data type (" + dataType + ": " + column.dataTypeName + ")");
			}
		}

		// If we are performing classification, treat the target column differently
		if ("classification".equals(setting.task) && setting.targetTable.equals(name)) {

			// If the target is a String, store the information.
			// NOTE: I AM NOT SURE I NEED THIS INFORMATION
			if (columnMap.get(setting.targetColumn).isNominal) {
				setting.isTargetString = true;
			}

			// Target column shall always be considered nominal when doing classification (useful for patterns like "WoE").
			columnMap.get(setting.targetColumn).isNominal = true;
		}

		// Output quality control
		if (columnCounter > 0) {
			logger.warn("In total " + columnCounter + " columns were ignored in " + name + " because of unsupported data type.");
		}

	}

	// Identify ids based on foreign key constraints
	public void identifyId() {
		// Add ids present in FK constrains
		for (ForeignConstraint relation : foreignConstraintList) {
			for (String idColumnName : relation.column) {
				if (columnMap.containsKey(idColumnName)) {
					Column column = columnMap.get(idColumnName);    // Find the id column
					column.isId = true;                             // And set it as an Id
				} else {
					logger.info("Attribute " + name + "." + idColumnName + ", which is used in a foreign key constraint, is disabled in the configuration. This may result into unreachable tables.");
				}
			}
		}
	}

	@Override
	public String toString() {
		return (name + ": " + getColumns());
	}
}
