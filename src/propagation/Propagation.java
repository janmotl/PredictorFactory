package propagation;


import connection.Network;
import meta.*;
import org.apache.log4j.Logger;
import run.Setting;
import utility.Hash;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Propagation {
	// Logging
	private static final Logger logger = Logger.getLogger(Propagation.class.getName());

	private static int counter;   // We want to guarantee that the journal_table is going to have unique and monotonically growing ids even if the table propagation fails -> it is not enough to just call propagated.size()+newlyPropagated.size()

	// Propagate tuples from the base table into all other tables.
	// The propagation depth is limited by setting.propagationDepthMax.
	// SHOULD CREATE SEVERAL tables if a cycle is present. So far only the farthest table is duplicated.
	// Note: Maybe I could pass base OutputTable to this method and not build it from scratch (just return outputTable
	// from some of the previously called methods).
	public static List<OutputTable> propagateBase(Setting setting, Database inputMeta) {
		// Initialize set of propagated tables and not-propagated samples
		List<Table> notPropagated = inputMeta.getAllTables();   // Set of tables to propagate
		List<OutputTable> propagated = new ArrayList<>();       // Set of propagated tables
		counter = 1;                                            // In SQL world it is customary to start counting from 1

		// Initialize the propagated set with baseSampled (we need a starting point for BFS)
		OutputTable base = new OutputTable();
		base.name = setting.baseSampled;
		base.schemaName = setting.outputSchema;
		base.originalName = setting.baseSampled;

		for (String columnName : setting.baseIdList) {  // Can be a composite Id
			Column column = new Column(columnName);
			base.columnMap.put(columnName, column);
		}

		Column temporalColumn = new Column(setting.baseDate);
		temporalColumn.isTemporal = true;
		base.columnMap.put(setting.baseDate, temporalColumn);

		for (String columnName : setting.baseTargetList) {  // We can have multiple targets
			Column column = new Column(columnName);
			base.columnMap.put(columnName, column);
		}

		ForeignConstraint fc = new ForeignConstraint("FK_baseTable_targetTable", setting.targetSchema, setting.targetTable, setting.outputSchema, setting.baseSampled, setting.targetIdList, setting.baseIdList);
		base.foreignConstraintList.add(fc);

		propagated.add(base);   // Temporarily add base table...


		// Get an estimate of range of targetDate
		if (setting.targetDate != null) {
			setting.baseDateRange = setting.dialect.getDateRange(setting, setting.baseTable, setting.baseDate);
		}

		// Call BFS
		propagated = bfs(setting, 1, propagated, notPropagated);

		// Output QC: If the count of propagated tables is low, complain about it.
		if (propagated.size() < 1) {
			logger.warn("Count of propagated tables is 0. The base table itself will be returned.");
		}

		return propagated;
	}

	// Breadth First Search (BFS)
	// Loop over the current level twice.
	// The first loop processes all the nodes, the second loop recurses into all the non-leaf nodes.
	private static List<OutputTable> bfs(Setting setting, int depth, List<OutputTable> propagated, List<Table> notPropagated) {

		// Initialization
		List<OutputTable> newlyPropagated = new ArrayList<>();      // Set of tables propagated at the current depth
		List<Table> stillNotPropagated = new ArrayList<>(notPropagated);  // Set of tables to propagate (it is not enough to just visit each table, we want to walk over each edge!)

		// Loop over tables
		for (OutputTable table1 : propagated) {
			for (Table table2 : notPropagated) {

				// Get relationships between table1 and table2
				List<ForeignConstraint> relationshipList = table1.getMatchingFKCs(table2);

				// Loop over all relationships between the tables
				for (ForeignConstraint relationship : relationshipList) {
					// Initialize
					OutputTable table = new OutputTable(table2);
					table.propagationOrder = counter;
					table.propagationForeignConstraint = relationship;
					table.propagationTable = table1.name;
					table.propagationPath = getPropagationPath(table1);
					table.propagationTables = getPropagationTables(table1);
					table.name = trim(setting, table, table2.name); // We have to make sure that outputTable name is short enough but unique (we add id)
					table.dateBottomBounded = true;    // NOT PROPERLY USED - is a constant

					// Identify time constraint
					table = TemporalConstraint.find(setting, table);

					// Make a new table
					table.sql = setting.dialect.propagateID(setting, table);
					table.isSuccessfullyExecuted = Network.executeUpdate(setting.dataSource, table.sql);

					// Get the row count
					// NOTE: Should be conditional on isSuccessfullyPropagated (also below).
					table.rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, table.name);
					logger.debug("Table \"" + table.originalName + "\" with \"" + table.temporalConstraint + "\" time constraint has " + table.rowCount + " rows.");

					// If the produced table has 0 rows and time constraint was used, repeat without any time constraint.
					if (table.rowCount == 0 && table.temporalConstraint != null) {
						setting.dialect.dropTable(setting, table.name);
						table.temporalConstraint = null;
						table.temporalConstraintJustification = "The attempt to use time constraint failed - no row satisfies the time frame defined in the initial setting of Predictor Factory.";

						// Make a new table
						table.sql = setting.dialect.propagateID(setting, table);
						table.isSuccessfullyExecuted = Network.executeUpdate(setting.dataSource, table.sql);

						// Get the row count
						table.rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, table.name);
						logger.debug("Table \"" + table.originalName + "\" with \"" + table.temporalConstraint + "\" time constraint has " + table.rowCount + " rows.");
					}

					// Add indexes
					setting.dialect.addIndex(setting, table.name);

					// Is OK?
					table.isOk = (table.isSuccessfullyExecuted && table.rowCount > 0);

					// Update the state
					stillNotPropagated.remove(table2);
					if (table.isOk) {
						newlyPropagated.add(table);
					}

					// Collect metadata
					table.isTargetIdUnique = setting.dialect.isTargetIdUnique(setting, table.name);
					table.timestampDelivered = LocalDateTime.now();

					// Log it
					setting.dialect.addToJournalTable(setting, table);

					// Increment
					counter++;
				}
			}
		}

		// If we didn't propagate ANY new table (e.g. there are no foreign key constraints to the remaining tables) or
		// we propagated all the tables or we reached the maximal propagation depth, return.
		if (newlyPropagated.isEmpty() || stillNotPropagated.isEmpty() || depth >= setting.propagationDepthMax) {
			// Output quality control
			if (!stillNotPropagated.isEmpty()) {
				logger.warn("Following tables were not propagated: " + stillNotPropagated.toString());
				logger.info("The maximal allowed propagation depth is: " + setting.propagationDepthMax);
			}
			return newlyPropagated;
		}

		// Otherwise go a level deeper.
		logger.info("#### Propagated the base table to depth: " + depth + " ####");
        newlyPropagated.addAll(bfs(setting, ++depth, newlyPropagated, stillNotPropagated));
		return newlyPropagated; // Includes newly propagated and all propagated from deeper levels
	}

	// Trim the length of a table name to the length permitted by the database
	// but make sure the name is unique.
	private static String trim(Setting setting, OutputTable table, String outputTable) {
		outputTable = setting.propagatedPrefix + "_" + outputTable;
		int stringLength = Math.min(outputTable.length(), setting.identifierLengthMax - 4);
		outputTable = outputTable.substring(0, stringLength);
		outputTable = outputTable + "_" + Hash.deterministically(table);

		return outputTable;
	}

	private static List<String> getPropagationPath(OutputTable table) {
		List<String> path = new ArrayList<>(table.propagationPath);
		path.add(table.originalName);
		return path;
	}

	private static List<OutputTable> getPropagationTables(OutputTable table) {
		List<OutputTable> list = new ArrayList<>(table.propagationTables);
		list.add(table);
		return list;
	}
}
