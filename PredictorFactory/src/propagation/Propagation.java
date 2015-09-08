package propagation;


import com.rits.cloning.Cloner;
import connection.Network;
import connection.SQL;
import metaInformation.ForeignConstraint;
import metaInformation.MetaOutput.OutputTable;
import org.apache.log4j.Logger;
import run.Setting;
import utility.Meta.Table;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class Propagation{
	// Logging
	private static final Logger logger = Logger.getLogger(Propagation.class.getName());

	// Deep cloning
	private static Cloner cloner = new Cloner();

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
		ForeignConstraint fc = new ForeignConstraint("FK_baseTable_targetTable",setting.baseTable, setting.targetTable, setting.baseIdList, setting.targetIdList);
		base.foreignConstraintList.add(fc);
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
		Set<String> newlyPropagated = new HashSet<>();		// Set of tables propagated at the current depth
		Set<String> stillNotPropagated = new HashSet<>(notPropagated);	// Set of tables to propagate

		// Loop over tables
		for (String table1 : propagated) {
			for (String table2 : notPropagated) {

				// Get time columns (from table2)
				SortedSet<String> timeSet = metaInput.get(table2).timeColumn;
				
				// Get relationships between table1 and table2
				List<ForeignConstraint> relationshipList = metaOutput.get(table1).foreignConstraintList.stream().filter(propagationForeignConstraint -> table2.equals(propagationForeignConstraint.fTable)).collect(Collectors.toList());

				for (ForeignConstraint relationship : relationshipList) {
					// Initialize
					OutputTable table = new OutputTable();
					table.timeColumn = metaInput.get(table2).timeColumn;
					table.nominalColumn = metaInput.get(table2).nominalColumn;
					table.numericalColumn = metaInput.get(table2).numericalColumn;
					table.idColumn = metaInput.get(table2).idColumn;
					table.foreignConstraintList = metaInput.get(table2).foreignConstraintList;
					table.uniqueList = metaInput.get(table2).uniqueList;

					table.propagatedName = trim(setting, table2, metaOutput.size()); // We have to make sure that outputTable name is short enough but unique (we add id)
					table.originalName = table2;
					table.propagationTable = table1;
					table.propagationPath = getPropagationPath(metaOutput.get(table1));
					table.propagationForeignConstraint = relationship;
					table.dateBottomBounded = false;

					// If idColumn2 is distinct in table2, it is unnecessary to set time condition.
					// For example, in Customer table, which contains only static information, like Birth_date,
					// it is not necessary to set the time constrain.
					table.isIdUnique = SQL.isIdUnique(setting, table);
					
					// Attempt to use time constrain
					if (setting.targetDate != null & !table.isIdUnique & !timeSet.isEmpty() & timeSet.size()<3) {
						// Initialize list of candidate tables
						ArrayList<OutputTable> candidateList = new ArrayList<>();
						
						// Generate a table for each date
						for (String date : timeSet) {
							// Redefine parameters
							table.propagatedName = trim(setting, table2, metaOutput.size() + candidateList.size());
							table.constrainDate = date;
							table.dateBottomBounded = true;
							
							// Make a new table
							table.sql = SQL.propagateID(setting, table);
							Network.executeUpdate(setting.connection, table.sql);

							// Get the row count
							table.rowCount = SQL.getRowCount(setting, setting.outputSchema, table.propagatedName);

							// If the row count is 0, drop the table
							if  (table.rowCount == 0) {
								SQL.dropTable(setting, table.propagatedName);
							} else {
								candidateList.add(cloner.deepClone(table));
							}
						}

						// If all attempts failed, do not use any time constrain
						if (candidateList.isEmpty()) {
							table.propagatedName = trim(setting, table2, metaOutput.size());
							table.constrainDate = null;
							table.dateBottomBounded = false;
							table.sql =  SQL.propagateID(setting, table);
							Network.executeUpdate(setting.connection, table.sql);
							table.rowCount = SQL.getRowCount(setting, setting.outputSchema, table.propagatedName);
						} else {
							table = candidateList.get(0);	// Just pick the first suitable table
						}

					} else {	// Propagate without the date constrain
						table.sql =  SQL.propagateID(setting, table);
						Network.executeUpdate(setting.connection, table.sql);
						table.rowCount = SQL.getRowCount(setting, setting.outputSchema, table.propagatedName);
					} 

					// Add indexes
					SQL.addIndex(setting, table.propagatedName);

					// Is OK?
					table.isOk = (table.rowCount > 0 ? true : false);

					// Update the state
					stillNotPropagated.remove(table2);
					if (table.isOk) {
						newlyPropagated.add(table.propagatedName);
					}

					// Collect metadata
					table.isUnique = SQL.isUnique(setting, table.propagatedName, false);
					table.propagationOrder = metaOutput.size();
					table.timestampDelivered = LocalDateTime.now();

					// Add the table into tableMetadata list
					metaOutput.put(table.propagatedName, table);

					// Log it
					SQL.addToJournalPropagation(setting, table);
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

	private static List<String> getPropagationPath(OutputTable table) {
		List<String> path = new ArrayList<>(table.propagationPath);
		path.add(table.originalName);
		return path;
	}
}
