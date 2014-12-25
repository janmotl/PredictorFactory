package run;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import utility.SQL;

// Table metadata
public class Metadata {
	
	// Define struct
	public static class Table {
		Set<String> numericalColumn;	// Aggregable columns
		Set<String> nominalColumn;		// Categorical columns
		Set<String> dateColumn;			// Time, date, datetime, timestamp...
		Set<String> dataColumn;			// All but ids
		Set<String> anyColumn;			// All columns
		Set<String> idColumn;			// Just ids
		int cardinality;				// Does combination {baseId, baseDate} repeat?
		String originalName;			// The table name before propagation
		String propagatedName;			// The table name after propagation
		String pathName;				// Like propagated name, but without the prefix. Useful for predictor naming.
		String propagationDate;			// Column used for time constrain during table propagation (in the last table).
		List<String> propagationPath;	// In the case of loops this makes the difference.
		boolean propagated;
	}
	
	public static Map<String, Table> outputList;
	
	
	// Get a list of tables with metainformation
	// WE ARE CONSIDERING PROPAGATED TABLES - in baseId...
	public static SortedMap<String, Table> getMetadata(Setting setting, SortedMap<String, Table> tableMetadata){

		for (Table table : tableMetadata.values()) {
			table.numericalColumn = new TreeSet<String>(SQL.getColumnList(setting, table.propagatedName, "number"));
			table.numericalColumn.removeAll(getIDColumnList(table.numericalColumn));
			
			// THIS IS NOT EXACT. SOME NUMERICAL ATTRIBUTES ARE ALSO NOMINAL.
			table.nominalColumn = new TreeSet<String>(SQL.getColumnList(setting, table.propagatedName, "string"));
			table.nominalColumn.removeAll(getIDColumnList(table.nominalColumn));
			
			table.dateColumn = new TreeSet<String>(SQL.getColumnList(setting, table.propagatedName, "date"));
			table.dateColumn.removeAll(getIDColumnList(table.dateColumn));
			
			table.anyColumn = new TreeSet<String>(SQL.getColumnList(setting, table.propagatedName, "any"));
			table.idColumn = getIDColumnList(table.anyColumn);
			table.dataColumn = new TreeSet<String>(table.anyColumn);
			table.dataColumn.removeAll(table.idColumn);
		}
		
		return tableMetadata;
	}
	
	// Subroutine: Return only ID columns
	private static Set<String> getIDColumnList(Set<String> columnList){
		Set<String> result = new TreeSet<String>(); 
		for (String columnName : columnList) {
			String columnNameCI = columnName.toUpperCase(Locale.ROOT);	// Case insensitive search.
			if (columnNameCI.startsWith("ID") || columnNameCI.endsWith("ID") || columnNameCI.endsWith("_NO") ) result.add(columnName);
		}
		return result;
	}
	
}
