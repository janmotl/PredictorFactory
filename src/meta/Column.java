package meta;

/*
This class describes an attribute (column) in a database.
When the class is created, it is populated with JDBC DatabaseMetadata.getColumns() call. Hence, all
public fields should be populated after the initialization. The class also contains potentially expensive
calls like nullability or uniqueness validation tests. Hence, these properties are hidden behind function
calls and memoized.
*/

import run.Setting;

import java.util.Set;
import java.util.TreeSet;

public class Column implements Comparable<Column> {
	// Final variables
	private final String schemaName;    // Necessity for memoized calls
	private final String tableName;     // Necessity for memoized calls
	public final String name;           // We have name field even though Column object is in a Map because it allows an easy iteration over the Columns (and not over that ugly Entry).
	public final int dataType;          // Data type as defined by JDBC
	public final String dataTypeName;   // Data type name as defined by database
	public final boolean isNullable;    // From JDBC


	// NOTE: Should be memoized! (Currently it is set in Schema during metadata load. Has to be profiled what is better.)
	public boolean isUnique;        // Does this column contain only unique values? NOTE: Currently not used

	// An integer column with low cardinality (<100) can be both, numerical and nominal.
	// An integer id column can be treated as an id, nominal and numerical.
	// Hence, these fields are not exclusive.
	public boolean isNominal;       // Categorical columns
	public boolean isNumerical;     // Additive columns
	public boolean isTemporal;      // Time, date, datetime, timestamp...
	public boolean isId;            // Foreign and primary keys

	// Memoized fields retrievable through function calls
	private Boolean containsNull;
	private Boolean containsFutureDate;
	private Set<String> uniqueValueSet;


	// Constructors
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
	}

	public Column(String schemaName, String tableName, String columnName, int dataType, String dataTypeName, boolean isNullable) {
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
	public Set<String> getUniqueValues(Setting setting) {
		// Memoized
		if (uniqueValueSet == null) {
			uniqueValueSet = new TreeSet<>(setting.dialect.getTopUniqueRecords(setting, schemaName, tableName, name));
		}
		return uniqueValueSet;
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
