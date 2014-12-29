package run;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import run.Metadata.Table;
import utility.Network;
import utility.SQL;

public class Propagation{

	// Propagate tuples from the base table into all other tables
	// BLIND APPROACH IS IMPLEMENTED - get list of PK-FK pairs
	// THE SEARCH DEBTH SHOULD BE LIMITED
	// SHOULD CREATE SEVERAL tables if a cycle is present. So far only the farthest table is duplicated. 
	// PROPAGATED TABLES SHOULD HAVE BEEN INDEXED
	public static SortedMap<String, Metadata.Table> propagateBase(Setting setting) {
		// Initialize
		ArrayList<String> tableList = Network.executeQuery(setting.connection, SQL.getTableList(setting, false));
		Set<String> notPropagated = new HashSet<String>(tableList); 		// List of tables to propagate
		Set<String> propagated = new HashSet<String>(); 					// List of propagated tables
		propagated.add(setting.baseTable);
		SortedMap<String, Metadata.Table> tableMetadata = new TreeMap<String, Metadata.Table>(); // The output list (based on the propagated name)
		Table base = new Table();	// Temporarily add base table (used in propagationPath building)...
		base.propagationPath = new ArrayList<String>();
		base.originalName = "base";
		tableMetadata.put(setting.baseTable, base);	//... into tableMetadata.
		
		// Call BFS
		tableMetadata = bfs(setting, 1, propagated, notPropagated, tableMetadata);
		
		// Remove base table (as we want a map of propagated tables)
		tableMetadata.remove(setting.baseTable);
		
		return tableMetadata;
	}

	// Breadth First Search (BFS)
	private static SortedMap<String, Metadata.Table> bfs(Setting setting, int depth, Set<String> propagated, Set<String> notPropagated, SortedMap<String, Metadata.Table> tableMetadata) {

		// Initialization
		Set<String> newlyPropagated = new HashSet<String>();		// Set of tables propagated at the current depth
		Set<String> stillNotPropagated = new HashSet<String>(notPropagated);	// Set of tables to propagate

		for (String table1 : propagated) {
			for (String table2 : notPropagated) {	
				
				// Get dateList (from table2)
				ArrayList<String> dateList = SQL.getColumnList(setting, table2, "date");
				
				// Get idList (from table1 intersect table2) 
				ArrayList<String> columnList = SQL.getSharedColumns(setting, table1, table2); 
				ArrayList<String> idList = Launcher.getIDColumnList(columnList);
			
				for (String id : idList) {
					// Define parameters
					boolean isPropagated = false;	// True if the query finished successfully 
					String outputTable;					
					Map<String, String> hashMap = new HashMap<String, String>();
					
					// During the first run only baseTable is "propagated". And baseTable is using special prefixes.
					// The prefix on the propagated id is desirable because sometimes all keys are named "ID".
					// And then the propagated table would have to have 2 "ID" columns if I wanted to preserve 
					// both of them. And I want to preserve both of them as "baseId" is necessary for aggregate
					// pattern and the table's "ID" is necessary for patterns relying on IDs.
					if (depth==1) {
						hashMap.put("@idColumn1", setting.baseId); 		// Use id with the prefix 
						hashMap.put("@idColumn2", setting.idColumn); 	// Use id without the prefix 
						outputTable = setting.propagatedPrefix + table2;	// Output table name 
					} else {
						hashMap.put("@idColumn1", id);
						hashMap.put("@idColumn2", id); 	
						outputTable = table1 + "_" + table2;	// Output table name with propagation path
					}
					hashMap.put("@inputTable1", table1);
					hashMap.put("@inputTable2", table2);
					hashMap.put("@outputTable", outputTable);
					
					// If idColumn2 is distinct in table2, it is unnecessary to set time condition.
					// For example, in Customer table, which contains only static information, like Birth_date,
					// it is not necessary to set the time constrain.
					int maxCardinality = SQL.getCardinality(setting, hashMap);
					
					// Use time constrain
					if (maxCardinality>1 & !dateList.isEmpty()) {
						// For each date
						for (String date : dateList) {
							// Define parameters
							outputTable = setting.propagatedPrefix + table2 + "_" + date;	// add date suffix
							hashMap.put("@outputTable", outputTable);
							hashMap.put("@dateColumn", date);
							
							// Make new table
							isPropagated =  SQL.propagateID(setting, hashMap, true); // bottom bounded
						}
				
						// If each date condition on the table results into an empty table, ignore the time bound.
						// This exception is handy, if the table contains date_birth column and not other date column.
						// I DISLIKE THE IDEA OF USING SQL HERE
						List<String> TableCheckList = SQL.getTableList(setting, "TABLE_NAME like '" + setting.propagatedPrefix + table2 + "_%" + "'");
						
						boolean isEmpty = true;
						for (String tableCheck : TableCheckList) {
							int rowCount = Integer.valueOf(Network.executeQuery(setting.connection, SQL.getRowCount(setting, tableCheck)).get(0));
							if (rowCount != 0) {
								isEmpty = false;
								break;
							};
						}
						
						if (isEmpty) {
							for (String tableCheck : TableCheckList) {
								Network.executeUpdate(setting.connection, SQL.getDropTable(setting, tableCheck));
							}
							
							// remove date suffix
							if (depth==1) {
								outputTable = setting.propagatedPrefix + table2;	
							} else {
								outputTable = table1 + "_" + table2;
							}
							
							hashMap.put("@outputTable", outputTable);
							hashMap.remove("@dateColumn");	
							isPropagated =  SQL.propagateID(setting, hashMap, true);
						}
					}
					
					
			
					// DateList can be empty. Or the cardinality is 1. In that case propagate without the date constrain.
					if (dateList.isEmpty() || maxCardinality==1) {	
						isPropagated =  SQL.propagateID(setting, hashMap, true);
					} 
			
					// Evaluate
					if (isPropagated) {
						stillNotPropagated.remove(table2);
						newlyPropagated.add(outputTable);
						
						// Add the table into tableMetadata list
						Metadata.Table table = new Metadata.Table();
						table.originalName = table2;
						table.propagatedName = outputTable;
						table.cardinality = maxCardinality;
						table.propagationDate = hashMap.get("@dateColumn");
						List<String> path = new ArrayList<String>(tableMetadata.get(table1).propagationPath); // Add copy propagation path from table1...
						path.add(tableMetadata.get(table1).originalName); 	//... and add table1 name...
						table.propagationPath = path; 						// ...to the table.
						tableMetadata.put(outputTable, table);	
					}
				}
			}
		}
		
		// If we didn't propagated ANY new table (i.e. tables are truly independent of each other) or we have 
		// propagated all the tables, return the conversionMap.
		if (newlyPropagated.isEmpty() || stillNotPropagated.isEmpty()) {
			return tableMetadata;
		}
		
		// Otherwise go a level deeper.
		System.out.println("#### Finished propagation at depth: " + depth + " ####");
		return bfs(setting, ++depth, newlyPropagated, stillNotPropagated, tableMetadata);		
	}
	
}
