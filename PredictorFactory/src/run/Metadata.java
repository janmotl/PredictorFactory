package run;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import utility.Network;
import utility.SQL;

public class Metadata {
	
	// Define struct
	public static class Table {
		Set<String> numericalColumn;
		Set<String> nominalColumn;
		Set<String> dateColumn;
		Set<String> dataColumn;
		Set<String> anyColumn;
		Set<String> idColumn;
		int cardinality;
		boolean propagated;
	}
	
	public static Map<String, Table> outputList;
	
	
	// Get a list of tables with metainformation
	// WE ARE CONSIDERING PROPAGATED TABLES - in baseId...
	public static Map<String, Table> getMetadata(Setting setting){
		// Make a list of tables
		ArrayList<String> tableList = Network.executeQuery(setting.connection, SQL.getTableList(setting, true));
		outputList = new HashMap<String, Table>(tableList.size()); 
		for (String table : tableList) {
			Table newTable = new Table();
			newTable.numericalColumn = new TreeSet<String>(SQL.getColumnList(setting, table, "number"));
			newTable.numericalColumn.removeAll(getIDColumnList(newTable.numericalColumn));
			
			// THIS IS NOT EXACT. SOME NUMERICAL ATTRIBUTES ARE ALSO NOMINAL.
			newTable.nominalColumn = new TreeSet<String>(SQL.getColumnList(setting, table, "string"));
			newTable.nominalColumn.removeAll(getIDColumnList(newTable.nominalColumn));
			
			newTable.dateColumn = new TreeSet<String>(SQL.getColumnList(setting, table, "date"));
			newTable.dateColumn.removeAll(getIDColumnList(newTable.dateColumn));
			
			newTable.anyColumn = new TreeSet<String>(SQL.getColumnList(setting, table, "any"));
			newTable.idColumn = getIDColumnList(newTable.anyColumn);
			newTable.dataColumn = new TreeSet<String>(newTable.anyColumn);
			newTable.dataColumn.removeAll(newTable.idColumn);
			
			// Get cardinality
			Map<String, String> map = new HashMap<String, String>();
			map.put("@inputTable2", table);
			map.put("@idColumn2", setting.baseId);
			newTable.cardinality = SQL.getCardinality(setting, map);
			
			outputList.put(table, newTable);
		}
		
		return outputList;
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
