package run;

import java.util.List;
import java.util.Locale;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import utility.SQL;

// Table metadata
public class Metadata {
	
	// Define struct
	public static class Table {
		SortedSet<String> numericalColumn;	// Aggregable columns (sorted to make selection of a single element concise)
		SortedSet<String> nominalColumn;	// Categorical columns
		SortedSet<String> dateColumn;		// Time, date, datetime, timestamp...
		SortedSet<String> dataColumn;		// All but ids
		SortedSet<String> anyColumn;		// All columns
		SortedSet<String> idColumn;			// Just ids
		int idCardinality;				// Does the propagation id repeat in the table?
		boolean isUnique;				// Does combination {baseId, baseDate} repeat?
		String originalName;			// The table name before propagation
		String propagatedName;			// The table name after propagation
		String pathName;				// Like propagated name, but without the prefix. Useful for predictor naming.
		String propagationDate;			// Column used for time constrain during table propagation (in the last table).
		List<String> propagationPath;	// In the case of loops this makes the difference.
		boolean propagated;
	}
	
	public static SortedMap<String, Table> OutputList;
	
	
	// Get a list of tables with metainformation
	// WE ARE CONSIDERING PROPAGATED TABLES - in baseId...
	public static SortedMap<String, Table> getMetadata(Setting setting, SortedMap<String, Table> tableMetadata){

		for (Table table : tableMetadata.values()) {
			table.numericalColumn = SQL.getColumnList(setting, table.propagatedName, "number");
			table.numericalColumn.removeAll(getIDColumnList(table.numericalColumn));
			
			// THIS IS NOT EXACT. SOME NUMERICAL ATTRIBUTES ARE ALSO NOMINAL.
			table.nominalColumn = SQL.getColumnList(setting, table.propagatedName, "string");
			table.nominalColumn.removeAll(getIDColumnList(table.nominalColumn));
			
			table.dateColumn = SQL.getColumnList(setting, table.propagatedName, "date");
			table.dateColumn.removeAll(getIDColumnList(table.dateColumn));
			
			table.anyColumn = SQL.getColumnList(setting, table.propagatedName, "any");
			table.idColumn = getIDColumnList(table.anyColumn);
			table.dataColumn = table.anyColumn;
			table.dataColumn.removeAll(table.idColumn);
			
			table.isUnique = SQL.isUnique(setting, table.propagatedName);
		}
		
		return tableMetadata;
	}
	
	// Subroutine: Return only ID columns
	private static SortedSet<String> getIDColumnList(SortedSet<String> columnList){
		SortedSet<String> result = new TreeSet<String>(); 
		for (String columnName : columnList) {
			String columnNameCI = columnName.toUpperCase(Locale.ROOT);	// Case insensitive search.
			if (columnNameCI.startsWith("ID") || columnNameCI.endsWith("ID") || columnNameCI.endsWith("_NO") ) result.add(columnName);
		}
		return result;
	}
	
}
