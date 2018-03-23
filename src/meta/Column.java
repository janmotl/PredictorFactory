package meta;

/*
This class describes an attribute (column) in a database.
When the class is created, it is populated with JDBC DatabaseMetadata.getColumns() call. Hence, all
public fields should be populated after the initialization. The class also contains potentially expensive
calls like nullability or uniqueness validation tests. Hence, these properties are hidden behind function
calls and memoized.
*/

import run.Setting;

import java.util.LinkedHashMap;

public class Column implements Comparable<Column> {
	public final String name;           // We have name field even though Column object is in a Map because it allows an easy iteration over the Columns (and not over that ugly Entry).
	public final int dataType;          // Data type as defined by JDBC
	public final String dataTypeName;   // Data type name as defined by database
	public final boolean isNullable;    // From JDBC
	public final boolean isDecimal;     // From JDBC
	// Final variables
	private final String schemaName;    // Necessity for memoized calls
	private final String tableName;     // Necessity for memoized calls
	// NOTE: Should be memoized! (Currently it is set in Schema during metadata load. Has to be profiled what is better.)
	public boolean isUnique;        // Does this column contain only unique values? NOTE: Currently not used

	// An integer column with low cardinality (<100) can be both, numerical and nominal.
	// An integer id column can be treated as an id, nominal and numerical.
	// Hence, these fields are not exclusive.
	public boolean isCharacter;     // Character columns
	public boolean isNominal;       // Categorical columns
	public boolean isNumerical;     // Additive columns
	public boolean isTemporal;      // Time, date, datetime, timestamp...
	public boolean isId;            // Foreign and primary keys

	// Memoized fields retrievable through function calls
	private Boolean containsNull;
	private Boolean containsFutureDate;
	private LinkedHashMap<String, Integer> uniqueValueMap; // Sorted by the occurrence count in decreasing order


	// Constructors
	@SuppressWarnings("unused")
	private Column() {
		this("No argument constructor is needed for unmarshalling of journal.xml with JAXB");
	}

	public Column(String name) {
		if (name == null) {
			throw new NullPointerException("The column name cannot be null");
		}

		this.schemaName = "Dummy";
		this.tableName = "Dummy";
		this.name = name;
		this.dataTypeName = "This is a phony data type for a phony instance from Propagation class";
		this.dataType = Integer.MIN_VALUE;
		this.isNullable = false;
		this.isDecimal = false;
	}

	public Column(String schemaName, String tableName, String columnName, int dataType, String dataTypeName, boolean isNullable, boolean isDecimal) {
		if (schemaName == null) {
			throw new NullPointerException("The schema name cannot be null");
		}
		if (tableName == null) {
			throw new NullPointerException("The column name cannot be null");
		}
		if (columnName == null) {
			throw new NullPointerException("The column name cannot be null");
		}
		if (dataTypeName == null) {
			throw new NullPointerException("The data type name cannot be null");
		}

		this.schemaName = schemaName;
		this.tableName = tableName;
		this.name = columnName;
		this.dataTypeName = dataTypeName;
		this.dataType = dataType;
		this.isNullable = isNullable;
		this.isDecimal = isDecimal;
	}


	// Does the column contain a null value?
	// Note: We do it here in Column because it allows us to memoize the result.
	// Use this method instead of SQL.containsNull() as this method is generally faster.
	public boolean containsNull(Setting setting) {
		// Memoized
		if (containsNull == null) {
			if (isNullable) {
				containsNull = setting.dialect.containsNull(setting, schemaName, tableName, name);
			} else {
				containsNull = false;   // We trust the database that it enforces the constrains. In Netezza, we trust the architect...
			}
		}
		return containsNull;
	}

	// Does the column contain an event that is in the future?
	public boolean containsFutureDate(Setting setting) {
		// Memoized
		if (containsFutureDate == null) {
			containsFutureDate = setting.dialect.containsFutureDate(setting, schemaName, tableName, name);
		}
		return containsFutureDate;
	}

	// Returns the top N most frequent unique values.
	// The unique values can be used in patterns like "WoE" or "Existential count" for nominal columns.
	// The values are sorted by their frequency.
	// Since we want to make WOE deterministic (to always pick the same reference value), we have to use a collection
	// that preserve order of the elements.
	public LinkedHashMap<String, Integer> getUniqueValues(Setting setting) {
		// Memoized
		if (uniqueValueMap == null) {
			uniqueValueMap = setting.dialect.getTopUniqueRecords(setting, schemaName, tableName, name);
		}
		return uniqueValueMap;
	}

	@Override
	public int compareTo(Column other) {
		return name.compareToIgnoreCase(other.name);
	}

	@Override
	public String toString() {
		return name;
	}
}
