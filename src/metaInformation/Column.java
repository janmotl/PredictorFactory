package metaInformation;

/*
This class describes an attribute (column) in a database.
When the class is created, it is populated with JDBC DatabaseMetadata.getColumns() call. Hence, all
public fields should be populated after the initialization. The class also contains potentially expensive
calls like nullability or uniqueness validation tests. Hence, these properties are hidden behind function
calls and memoized.
*/

import connection.SQL;
import org.jetbrains.annotations.NotNull;
import run.Setting;

import java.util.LinkedHashSet;
import java.util.Set;

public class Column implements Comparable<Column> {

    public int dataType;            // Data type as defined by JDBC
    public String dataTypeName;     // Data type as defined by database
    public boolean isNullable;      // From JDBC
    public boolean isUnique;        // From JDBC
    public String name;             // We have name field even though Column object is in a Map because it allows an easy iteration over the Columns (and not over that ugly Entry).
    public Set<String> uniqueValueSet = new LinkedHashSet<>();    // Top n most frequent values. The n is defined by setting.

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


    // Constructors
    public Column(String name) {
        if (name==null) {
            throw new NullPointerException("The column name is null");
        }

        this.name = name;
    }



    // Does the column contain a null value?
    // NOTE: Would not a better place for this in Meta?
    public Boolean containsNull(Setting setting, String tableName){
        // Memoized
        if (containsNull == null) {
            if (isNullable) {
                containsNull = SQL.containsNull(setting, tableName, name);
            } else {
                containsNull = false;   // We trust the database that it enforces the constrains. In Netezza, we trust the architect...
            }
        }
        return containsNull;
    }

    // Does the column contain an event that is in the future?
    public Boolean containsFutureDate(Setting setting, String tableName){
        // Memoized
        if (containsFutureDate == null) {
            containsFutureDate = SQL.containsFutureDate(setting, tableName, name);
        }
        return containsFutureDate;
    }

    @Override
    public int compareTo(@NotNull Column other) {
        return this.name.compareToIgnoreCase(other.name);
    }

    @Override
    public String toString() {
        return name;
    }
}
