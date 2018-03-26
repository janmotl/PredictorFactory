package meta;

import org.apache.log4j.Logger;
import run.Setting;

import javax.xml.bind.annotation.XmlID;
import java.util.*;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;


// Define struct.
// Always create collections to avoid null pointer exception and need to create collections at many places.
// TO MAKE LIVE NICER WE COULD MAKE THE MAPS PRIVATE AND IMPLEMENT FOLLOWING INTERFACE: {!!!get(columnName)!!!, add{column},
// addAll(Set<column>), retainAll(Set<columnName>), removeAll(Set<columnName>)}.
// WE SHOULD DO THE SAME FOR META: getTable(tableName)
public class Table {

	// Logging
	private static final Logger logger = Logger.getLogger(Table.class.getName());

	@XmlID public String name;  // To make Table self-sufficient even outside the map; annotated as an unique id (needed for marshalling into XML). BUT IT CAN FAIL IF MULTIPLE INPUT SCHEMAS ARE USED as then we have to use composite PK, for example with @XmlKey
	public String schemaName;
	public SortedMap<String, Column> columnMap = new TreeMap<>();   // All columns (but eventually blacklisted)
	public List<ForeignConstraint> foreignConstraintList = new ArrayList<>();   // NOTE: Should we create a single ForeignConstraintList?

	// Constructors
	public Table() {
	}

	public Table(String schemaName, String name) {
		requireNonNull(schemaName, "Schema name cannot be null");
		requireNonNull(name, "Table name cannot be null");
		this.schemaName = schemaName;
		this.name = name;
	}

	// It makes sens to store columns in a single collection to avoid duplication.
	// These getters are not the most efficiently implemented. Feel free to replace it with a multi-key map.
	// Or we may keep private string lists for each type.
	// We are returning sets, hence, it is easy to get unique columns that are nominal OR numerical...
	public SortedSet<Column> getColumns(Setting setting, StatisticalType statisticalType) {
		SortedSet<Column> result = new TreeSet<>();

		for (Column column : columnMap.values()) {
			if (statisticalType == StatisticalType.CHARACTER && column.isCharacter) result.add(column);
			if (statisticalType == StatisticalType.NOMINAL && column.isNominal) result.add(column);
			if (statisticalType == StatisticalType.NUMERICAL && column.isNumerical) result.add(column);
			if (statisticalType == StatisticalType.TEMPORAL && column.isTemporal) result.add(column);

			if (statisticalType == StatisticalType.BIGINT && column.dataType == -5) result.add(column);
			if (statisticalType == StatisticalType.BOOLEAN && column.dataType == 16) result.add(column);
			if (statisticalType == StatisticalType.CHAR && column.dataType == 1) result.add(column);
			if (statisticalType == StatisticalType.DATE && column.dataType == 91) result.add(column);
			if (statisticalType == StatisticalType.DECIMAL && column.dataType == 3) result.add(column);
			if (statisticalType == StatisticalType.DOUBLE && column.dataType == 8) result.add(column);
			if (statisticalType == StatisticalType.FLOAT && column.dataType == 6) result.add(column);
			if (statisticalType == StatisticalType.INTEGER && column.dataType == 4) result.add(column);
			if (statisticalType == StatisticalType.LONGNVARCHAR&& column.dataType == -16) result.add(column);
			if (statisticalType == StatisticalType.LONGVARCHAR && column.dataType == -1) result.add(column);
			if (statisticalType == StatisticalType.NCHAR && column.dataType == -15) result.add(column);
			if (statisticalType == StatisticalType.NUMERIC && column.dataType == 2) result.add(column);
			if (statisticalType == StatisticalType.NVARCHAR && column.dataType == -9) result.add(column);
			if (statisticalType == StatisticalType.REAL && column.dataType == 7) result.add(column);
			if (statisticalType == StatisticalType.SMALLINT && column.dataType == 5) result.add(column);
			if (statisticalType == StatisticalType.TIME && column.dataType == 92) result.add(column);
			if (statisticalType == StatisticalType.TIME_WITH_TIMEZONE && column.dataType == 2013) result.add(column);
			if (statisticalType == StatisticalType.TIMESTAMP && column.dataType == 93) result.add(column);
			if (statisticalType == StatisticalType.TIMESTAMP_WITH_TIMEZONE && column.dataType == 2014) result.add(column);
			if (statisticalType == StatisticalType.TINYINT && column.dataType == -6) result.add(column);
			if (statisticalType == StatisticalType.VARCHAR && column.dataType == 12) result.add(column);

			if (statisticalType == StatisticalType.ENUM && column.dataTypeName.equals("ENUM")) result.add(column);
			if (statisticalType == StatisticalType.INTERVAL && column.dataTypeName.equals("INTERVAL")) result.add(column);
			if (statisticalType == StatisticalType.SET && column.dataTypeName.equals("SET")) result.add(column);
			if (statisticalType == StatisticalType.YEAR && column.dataTypeName.equals("YEAR")) result.add(column);
		}

		// Use ids for feature calculation? Nevertheless, always allow targetColumns.
		if (!setting.useIdAttributes) {
			result.removeIf(column -> column.isId && !(setting.targetColumnList.contains(column.name) && setting.targetTable.equals(name)));
		}

		return result;
	}

	// Return all columns that were not blacklisted
	public SortedSet<Column> getColumns() {
		SortedSet<Column> result = new TreeSet<>(columnMap.values());
		return result;
	}

	// If the column is not in the map, raise an exception
	public Column getColumn(String columnName) {
		// Could extract the value and check for NULL for speed up (http://stackoverflow.com/questions/3626752/key-existence-check-in-hashmap)
		if (!columnMap.containsKey(columnName))
			throw new IllegalArgumentException("The column " + columnName + " was not found in table " + name);
		return columnMap.get(columnName);
	}

	// Find relationships from THIS --> table2.
	public List<ForeignConstraint> getMatchingFKCs(Table table2) {
		List<ForeignConstraint> result = new ArrayList<>();

		for (ForeignConstraint fc : foreignConstraintList) {
			requireNonNull(table2.name, "Table name cannot be null");
			requireNonNull(table2.schemaName, "Schema name cannot be null");
			requireNonNull(fc.fTable, "Table name cannot be null");
			requireNonNull(fc.fSchema, "Schema name cannot be null");

			if ( fc.fTable.equals(table2.name) && fc.fSchema.equals(table2.schemaName) ) {
				result.add(fc);
			}
		}

		return result;
	}

	// Get numerical, nominal and time columns.
	// NOTE: Shouldn't it be done either lazily or during the construction?
	public void categorizeColumns(Setting setting) {
		// Parameter control
		if (columnMap == null || columnMap.isEmpty()) {
			logger.info("Table: " + name + " doesn't contain any column beside ids.");
			return;
		}

		// Initialization
		int columnCounter = 0;

		// Mark the columns. If the data type is not recognized, the column is ignored.
		// This is intentional because we do not know how to deal with blobs (dataType = 2004)...
		// And we don't want them to slow down the mining process -> do not propagate them.
		// Doc: https://docs.oracle.com/cd/A97337_01/ias102_otn/buslog.102/bc4j/bc_abcdatatypes.htm
		// Doc: http://alvinalexander.com/java/edu/pj/jdbc/recipes/ResultSet-ColumnType.shtml
		// Doc: http://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
		for (Column column : columnMap.values()) {
			int dataType = column.dataType;
			boolean isKnown = false;

			if (dataType == -16 || dataType == -15 || dataType == -9 || dataType == -1 || dataType == 1 || dataType == 12) {
				column.isCharacter = true;
				isKnown = true;
			}
			if (dataType == -16 || dataType == -15 || dataType == -9 || dataType == -7 || dataType == -6 || dataType == -5 ||  dataType == -1 || dataType == 1 || dataType == 4 || dataType == 5 || dataType == 12 || dataType == 16) {
				column.isNominal = true;
				isKnown = true;
			}
			if (dataType == -7 || dataType == -6 || dataType == -5 || (dataType >= 2 && dataType <= 8) || dataType == 16) {
				column.isNumerical = true;
				isKnown = true;
			}
			if (dataType == 91 || dataType == 92 || dataType == 93 || dataType == 2013 || dataType == 2014) {
				column.isTemporal = true;
				isKnown = true;
			}

			// JDBC categorize all these as OTHER (dataType=1111). But we like to differentiate between them.
			if ("ENUM".equals(column.dataTypeName.toUpperCase())) {
				column.isCharacter = true;
				column.isNominal = true;
				isKnown = true;
			}

			if ("INTERVAL".equals(column.dataTypeName.toUpperCase())) {
				isKnown = true;
			}

			if ("SET".equals(column.dataTypeName.toUpperCase())) {
				isKnown = true;
			}

			if ("YEAR".equals(column.dataTypeName.toUpperCase())) {
				column.isTemporal = true;
				column.isNumerical = true;
				isKnown = true;
			}

			if (!isKnown) {
				columnCounter++;
				logger.debug("Ignoring column: " + name + "." + column.name
						+ " because it is of an unsupported data type (" + dataType + ": " + column.dataTypeName + ")");
			}

			// If the numeric/decimal attribute has scale=0 or is used in PK/FK, mark it as nominal
			if ((dataType == 2 || dataType == 3) && (!column.isDecimal || column.isId)) {
				column.isNominal = true;
			}

		}

		// If we are performing classification, treat the target columns as nominal
		if ("classification".equals(setting.task) && setting.targetTable.equals(name)) {
			// Target columns shall always be considered nominal when doing classification (useful for patterns like "WoE").
			for (String target : setting.targetColumnList) {
				columnMap.get(target).isNominal = true;
			}
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
		return name;
	}

	@Override // Used in Propagation class
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Table table = (Table) o;
		return Objects.equals(name, table.name) &&
				Objects.equals(schemaName, table.schemaName);
	}

	@Override
	public int hashCode() {
		return hash(name, schemaName);
	}
}
