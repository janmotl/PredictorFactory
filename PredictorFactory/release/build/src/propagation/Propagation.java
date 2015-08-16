package propagation;


import connection.Network;
import connection.SQL;
import metaInformation.ForeignConstraint;
import metaInformation.MetaOutput.OutputTable;
import org.apache.log4j.Logger;
import run.Setting;
import utility.Meta.Table;

import java.util.*;
import java.util.stream.Collectors;

public class Propagation{
	// Logging
	private static final Logger logger = Logger.getLogger(Propagation.class.getName());

	// Propagate tuples from the base table into all other tables
	// BLIND APPROACH IS IMPLEMENTED - get list of PK-FK pairs
	// THE SEARCH DEPTH SHOULD BE LIMITED
	// SHOULD CREATE SEVERAL tables if a cycle is present. So far only the farthest table is duplicated. 
	// PROPAGATED TABLES SHOULD HAVE BEEN INDEXED
	public static SortedMap<String, OutputTable> propagateBase(Setting setting, SortedMap<String, Table> inputMeta) {
		// Initialize set of propagated tables and not-propagated samples
		Set<String> notPropagated = inputMeta.keySet();	// Set of tables to propagate
		Set<String> propagated = new HashSet<>(); 		// Set of propagated tables...
		propagated.add(setting.baseTable);				// ...initialized with baseSampled.
		
		// Initialize MetaOutput
		SortedMap<String, OutputTable> metaOutput = new TreeMap<>(); // The output list (based on the propagated name)
		OutputTable base = new OutputTable();	// Temporarily add base table (used in propagationPath building)...
		base.propagationPath = new ArrayList<String>();
		base.originalName = setting.baseSampled;
		base.idColumn.add(setting.baseId);
		base.timeColumn.add(setting.baseDate);
		base.nominalColumn.add(setting.baseTarget);	// NOT ALWAYS, but do we care if we are removing the table at the end?
		ForeignConstraint fc = new ForeignConstraint(setting.baseTable, setting.targetTable, setting.baseIdList, setting.targetIdList);
		base.foreignConstraint.add(fc);
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
	private static SortedMap<String, OutputTable> bfs(Setting setting, int depth, Set<String> propagated, Set<String> notPropagated, SortedMap<String, Table> metaInput, SortedMap<String, OutputTable> metaOutput) {

		// Initialization
		String sql = "";
		Set<String> newlyPropagated = new HashSet<String>();		// Set of tables propagated at the current depth
		Set<String> stillNotPropagated = new HashSet<String>(notPropagated);	// Set of tables to propagate

		for (String table1 : propagated) {
			for (String table2 : notPropagated) {	
				
				// Get time columns (from table2)
				SortedSet<String> timeSet = metaInput.get(table2).timeColumn;
				
				// Get relationships between table1 and table2
				List<ForeignConstraint> relationshipList = metaOutput.get(table1).foreignConstraint.stream().filter(foreignConstraint -> table2.equals(foreignConstraint.fTable)).collect(Collectors.toList());

				for (ForeignConstraint relation : relationshipList) {
					// Define parameters
					boolean isPropagated = false;	// True if the query finished successfully 
					String outputTable;				// The name of the generated table
					Map<String, String> parameterMap = relation.getMap(); // Maps: @variable -> value

					// We have to make sure that outputTable name is short enough but unique
					outputTable = trim(setting, table2, metaOutput.size());
					
					parameterMap.put("@propagatedTable", table1);
					parameterMap.put("@inputTable", table2);
					parameterMap.put("@outputTable", outputTable);
					
					// If idColumn2 is distinct in table2, it is unnecessary to set time condition.
					// For example, in Customer table, which contains only static information, like Birth_date,
					// it is not necessary to set the time constrain.
					boolean isIdUnique = SQL.isIdUnique(setting, parameterMap); // Cardinality of idColumn2
					
					// Use time constrain
					if (!isIdUnique & !timeSet.isEmpty()) {
						// Initialize list of generated tables
						ArrayList<String> tableCheckList = new ArrayList<>();
						
						// Generate a table for each date
						for (String date : timeSet) {
							// Define parameters
							outputTable = trim(setting, table2, metaOutput.size() + tableCheckList.size()); 
							parameterMap.put("@outputTable", outputTable);
							parameterMap.put("@dateColumn", date);
							
							// Make new table
							sql = SQL.propagateID(setting, parameterMap, true); // bottom bounded
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
							int rowCount = SQL.getRowCount(setting, setting.outputSchema, tableCheck);
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
							parameterMap.put("@outputTable", outputTable);
							parameterMap.remove("@dateColumn");
							sql =  SQL.propagateID(setting, parameterMap, true);
							isPropagated = Network.executeUpdate(setting.connection, sql);
						}
					}
					
					
			
					// DateList is empty. Or the cardinality is 1. In that case propagate without the date constrain.
					if (timeSet.isEmpty() || isIdUnique) {	
						sql =  SQL.propagateID(setting, parameterMap, true);
						isPropagated = Network.executeUpdate(setting.connection, sql);
					} 
			
					// Evaluate
					if (isPropagated) {
						stillNotPropagated.remove(table2);
						newlyPropagated.add(outputTable);
						
						// Add indexes
						SQL.addIndex(setting, outputTable);
						
						// Add the table into tableMetadata list
						OutputTable table = new OutputTable();
						table.originalName = table2;
						table.propagatedName = outputTable;
						table.isUnique = SQL.isUnique(setting, outputTable, false);
						table.propagationDate = parameterMap.get("@dateColumn");
						table.propagationOrder = metaOutput.size();
						table.sql = sql;
						
						table.timeColumn = metaInput.get(table2).timeColumn;
						table.nominalColumn = metaInput.get(table2).nominalColumn;
						table.numericalColumn = metaInput.get(table2).numericalColumn;
						table.idColumn = metaInput.get(table2).idColumn;
						table.foreignConstraint = metaInput.get(table2).foreignConstraint;
						table.uniqueList = metaInput.get(table2).uniqueList;
						
						List<String> path = new ArrayList<>(metaOutput.get(table1).propagationPath); // Add copy of the propagation path from table1...
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
		return bfs(setting, ++depth, newlyPropagated, stillNotPropagated, metaInput, metaOutput);
	}
	
	// Trim the length of a table name to the length permitted by the database
	// but make sure the name is unique.
	private static String trim(Setting setting, String outputTable, int counter) {
		outputTable = setting.propagatedPrefix + outputTable;
		int stringLength = Math.min(outputTable.length(), setting.identifierLengthMax - 4);
		outputTable = outputTable.substring(0, stringLength);
		outputTable = outputTable + "_" + String.format("%03d", counter);
		
		return outputTable;
	}
}
