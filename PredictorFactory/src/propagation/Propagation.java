package propagation;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import metaInformation.MetaOutput.OutputTable;

import org.apache.log4j.Logger;

import run.Setting;
import utility.Meta.Table;
import connection.Network;
import connection.SQL;

public class Propagation{
	// Logging
	private static final Logger logger = Logger.getLogger(Propagation.class.getName());

	// Propagate tuples from the base table into all other tables
	// BLIND APPROACH IS IMPLEMENTED - get list of PK-FK pairs
	// THE SEARCH DEBTH SHOULD BE LIMITED
	// SHOULD CREATE SEVERAL tables if a cycle is present. So far only the farthest table is duplicated. 
	// PROPAGATED TABLES SHOULD HAVE BEEN INDEXED
	public static SortedMap<String, OutputTable> propagateBase(Setting setting, SortedMap<String, Table> inputMeta) {
		// Initialize
		Set<String> notPropagated = inputMeta.keySet();		// Set of tables to propagate
		Set<String> propagated = new HashSet<String>(); 	// Set of propagated tables
		propagated.add(setting.baseTable);
		
		// MetaOutput
		SortedMap<String, OutputTable> metaOutput = new TreeMap<String, OutputTable>(); // The output list (based on the propagated name)
		OutputTable base = new OutputTable();	// Temporarily add base table (used in propagationPath building)...
		base.propagationPath = new ArrayList<String>();
		base.originalName = "base";
		base.idColumn.add(setting.baseId);
		base.timeColumn.add(setting.baseDate);
		base.nominalColumn.add(setting.baseTarget);	// NOT ALWAYS
		ArrayList<String> foreignKey = new ArrayList<String>();
		foreignKey.add(setting.targetTable);
		foreignKey.add(setting.baseId);
		foreignKey.add(setting.targetId);
		base.relationship.add(foreignKey);
		metaOutput.put(setting.baseTable, base);	//... into tableMetadata.
		
		// Call BFS
		metaOutput = bfs(setting, 1, propagated, notPropagated, inputMeta, metaOutput);
		
		// Remove base table (as we want a map of propagated tables)
		metaOutput.remove(setting.baseTable);
		
		// Output QC: If the count of propagated tables is low, complain about it.
		if (metaOutput.size()<1) {
			logger.warn("Count of propagated tables is 0. The base table itself will be returned.");
		}
		
		return metaOutput;
	}

	// Breadth First Search (BFS)
	// Loop over the current level twice. 
	// The first loop processes all the nodes, the second loop recurses into all the non-leaf nodes. 
	private static SortedMap<String, OutputTable> bfs(Setting setting, int depth, Set<String> propagated, Set<String> notPropagated, SortedMap<String, Table> inputMeta, SortedMap<String, OutputTable> metaOutput) {

		// Initialization
		String sql = "";
		Set<String> newlyPropagated = new HashSet<String>();		// Set of tables propagated at the current depth
		Set<String> stillNotPropagated = new HashSet<String>(notPropagated);	// Set of tables to propagate

		for (String table1 : propagated) {
			for (String table2 : notPropagated) {	
				
				// Get time columns (from table2)
				SortedSet<String> timeSet = inputMeta.get(table2).timeColumn;
				
				// Get map of relationships between table1 and table2
				SortedMap<String, String> idMap = new TreeMap<String, String>(); 
				for (List<String> relationshipList : metaOutput.get(table1).relationship) {
					if (table2.equals(relationshipList.get(0))) {
						idMap.put(relationshipList.get(1), relationshipList.get(2));
					}
				}
							
				for (String id1 : idMap.keySet()) {
					// Define parameters
					boolean isPropagated = false;	// True if the query finished successfully 
					String outputTable;					
					Map<String, String> hashMap = new HashMap<String, String>();
					
					// During the first run only baseTable is "propagated". And baseTable is using special prefixes.
					// The prefix on the propagated id is desirable because sometimes all keys are named "ID".
					// And then the propagated table would have to have 2 "ID" columns if I wanted to preserve 
					// both of them. And I want to preserve both of them as "baseId" is necessary for aggregate
					// pattern and the table's "ID" is necessary for patterns relying on IDs.
					if (depth == 1) {
						hashMap.put("@idColumn1", setting.baseId); 		// Use id with the prefix 
						hashMap.put("@idColumn2", setting.targetId); 	// Use id without the prefix 
					} else {
						hashMap.put("@idColumn1", id1);					// Id1 from table1 
						hashMap.put("@idColumn2", idMap.get(id1)); 		// Id2 from table2 
					}
					
					// We have to make sure that outputTable name is short enough but unique
					outputTable = trim(setting, table2, metaOutput.size());
					
					hashMap.put("@propagatedTable", table1);
					hashMap.put("@inputTable", table2);
					hashMap.put("@outputTable", outputTable);
					
					// If idColumn2 is distinct in table2, it is unnecessary to set time condition.
					// For example, in Customer table, which contains only static information, like Birth_date,
					// it is not necessary to set the time constrain.
					boolean isIdUnique = SQL.isIdUnique(setting, hashMap); // Cardinality of idColumn2
					
					// Use time constrain
					if (!isIdUnique & !timeSet.isEmpty()) {
						// Initialize list of generated tables
						ArrayList<String> tableCheckList = new ArrayList<String>();
						
						// Generate a table for each date
						for (String date : timeSet) {
							// Define parameters
							outputTable = trim(setting, table2, metaOutput.size() + tableCheckList.size()); 
							hashMap.put("@outputTable", outputTable);
							hashMap.put("@dateColumn", date);
							
							// Make new table
							sql = SQL.propagateID(setting, hashMap, true); // bottom bounded
							isPropagated = Network.executeUpdate(setting.connection, sql);
							
							// Log the result
							if (isPropagated) {
								tableCheckList.add(outputTable);
							}
						}
				
						// If each date condition on the table results into an empty table, ignore the time bound.
						// This exception is handy, if the table contains date_birth column and not other date column.
						boolean isEmpty = true;
						for (String tableCheck : tableCheckList) {
							int rowCount = SQL.getRowCount(setting, tableCheck);
							if (rowCount != 0) {
								isEmpty = false;
								break;
							}
						}
						
						if (isEmpty) {
							for (String tableCheck : tableCheckList) {
								SQL.dropTable(setting, tableCheck);
							}
							
							outputTable = trim(setting, table2, metaOutput.size());
							hashMap.put("@outputTable", outputTable);
							hashMap.remove("@dateColumn");	
							sql =  SQL.propagateID(setting, hashMap, true);
							isPropagated = Network.executeUpdate(setting.connection, sql);
						}
					}
					
					
			
					// DateList is empty. Or the cardinality is 1. In that case propagate without the date constrain.
					if (timeSet.isEmpty() || isIdUnique) {	
						sql =  SQL.propagateID(setting, hashMap, true);
						isPropagated = Network.executeUpdate(setting.connection, sql);
					} 
			
					// Evaluate
					if (isPropagated) {
						stillNotPropagated.remove(table2);
						newlyPropagated.add(outputTable);
						
						// Add the table into tableMetadata list
						OutputTable table = new OutputTable();
						table.originalName = table2;
						table.propagatedName = outputTable;
						table.isUnique = SQL.isUnique(setting, outputTable, false);
						table.propagationDate = hashMap.get("@dateColumn");
						table.propagationOrder = metaOutput.size();
						table.sql = sql;
						
						table.timeColumn = inputMeta.get(table2).timeColumn;
						table.nominalColumn = inputMeta.get(table2).nominalColumn;
						table.numericalColumn = inputMeta.get(table2).numericalColumn;
						table.idColumn = inputMeta.get(table2).idColumn;
						table.relationship = inputMeta.get(table2).relationship;
						table.uniqueList = inputMeta.get(table2).uniqueList;
						
						List<String> path = new ArrayList<String>(metaOutput.get(table1).propagationPath); // Add copy of the propagation path from table1...
						path.add(metaOutput.get(table1).originalName); 	//... and add table1 name...
						table.propagationPath = path; 						// ...to the table.
						metaOutput.put(outputTable, table);	
					}
				}
			}
		}
		
		// If we didn't propagated ANY new table (i.e. tables are truly independent of each other) or we have 
		// propagated all the tables or we have reached the maximal propagation depth, return the conversionMap.
		if (newlyPropagated.isEmpty() || stillNotPropagated.isEmpty() || depth>=setting.propagationDepthMax) {	
			// Output quality control
			if (!stillNotPropagated.isEmpty()) {
				logger.warn("Following tables were not propagated: " + stillNotPropagated.toString());
				logger.info("The maximal allowed propagation depth is: " + setting.propagationDepthMax); 
			}
			
			return metaOutput;
		}
		
		// Otherwise go a level deeper.
		logger.info("#### Finished propagation at depth: " + depth + " ####");
		return bfs(setting, ++depth, newlyPropagated, stillNotPropagated, inputMeta, metaOutput);		
	}
	
	// Trim the length of a table name to the length permitted by the database
	// but make sure the name is unique.
	private static String trim(Setting setting, String outputTable, int counter) {
		outputTable = setting.propagatedPrefix + outputTable;
		int stringLength = Math.min(outputTable.length(), setting.indentifierLengthMax - 4); 
		outputTable = outputTable.substring(0, stringLength);
		outputTable = outputTable + "_" + String.format("%03d", counter);
		
		return outputTable;
	}
}
