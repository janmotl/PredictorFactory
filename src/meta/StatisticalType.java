package meta;

// Data types beyond JDBC data types for convenience.
// Treat these types as a property set:
//  a binary attribute does not hold any of these properties,
//  but a year attribute can hold all these properties.
public enum StatisticalType{
	// Follows a subset of JDBC types as defined in Java 9:
	//  https://docs.oracle.com/javase/9/docs/api/constant-values.html#java.sql.Types.ARRAY
	LONGNVARCHAR,
	NCHAR,
	NVARCHAR,
	TINYINT,
	BIGINT,
	LONGVARCHAR,
	CHAR,
	NUMERIC,
	DECIMAL,
	INTEGER,
	SMALLINT,
	FLOAT,
	REAL,
	DOUBLE,
	VARCHAR,
	BOOLEAN,
	DATE,
	TIME,
	TIMESTAMP,
	SQLXML,
	TIME_WITH_TIMEZONE, // Equality (and calculation of differences) is broken for this type: https://stackoverflow.com/questions/20529284/postgres-time-with-time-zone-equality/20530283#20530283
	TIMESTAMP_WITH_TIMEZONE,

	// Following are currently categorized as OTHER in JDBC but it is convenient to introduce finer categorization.
	ENUM,
	INTERVAL,
	SET,
	YEAR,   // Is troublesome as datediff between year and timestamp does not work (in MySQL)

	// These are convenience union types.
	//	ID,         // Is in PK or FK
	ANY,        // e.g. for isNull pattern
	CHARACTER,  // e.g. for textLength pattern
	NOMINAL,    // We can group over this attribute
	NUMERICAL,  // Additive
	TEMPORAL    // Can be used to describe time
}