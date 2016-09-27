package propagation;


import com.rits.cloning.Cloner;
import connection.Network;
import connection.SQL;
import metaInformation.Column;
import metaInformation.ForeignConstraint;
import metaInformation.MetaOutput.OutputTable;
import org.apache.log4j.Logger;
import run.Setting;
import metaInformation.Table;

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
        Set<String> notPropagated = inputMeta.keySet(); // Set of tables to propagate
        Set<String> propagated = new HashSet<>();       // Set of propagated tables...
        propagated.add(setting.baseSampled);                // ...initialized with baseSampled.
    
        // Initialize MetaOutput
        SortedMap<String, OutputTable> metaOutput = new TreeMap<>(); // The output list (based on the propagated name)
        OutputTable base = new OutputTable();   // Temporarily add base table (used in propagationPath building)...
        base.propagationPath = new ArrayList<>();
        base.originalName = setting.baseSampled;

        for (String columnName : setting.baseIdList) {  // Can be a composite Id
            Column column = new Column(columnName);
            base.columnMap.put(columnName, column);
        }

        Column temporalColumn = new Column(setting.baseDate);
        temporalColumn.isTemporal = true;
        base.columnMap.put(setting.baseDate, temporalColumn);

        Column targetColumn = new Column(setting.baseTarget);
        base.columnMap.put(setting.baseTarget, targetColumn);

        ForeignConstraint fc = new ForeignConstraint("FK_baseTable_targetTable", setting.baseSampled, setting.targetTable, setting.baseIdList, setting.targetIdList);
        base.foreignConstraintList.add(fc);
        metaOutput.put(setting.baseSampled, base);  //... into tableMetadata.


        // Get an estimate of range of targetDate
        if (setting.targetDate != null) {
            setting.baseDateRange = SQL.getDateRange(setting, setting.baseTable, setting.baseDate);
        }
    
        // Call BFS
        metaOutput = bfs(setting, 1, propagated, notPropagated, inputMeta, metaOutput);
    
        // Remove base table (as we want a map of propagated tables)
        metaOutput.remove(setting.baseSampled);
    
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
        Set<String> newlyPropagated = new HashSet<>();      // Set of tables propagated at the current depth
        Set<String> stillNotPropagated = new HashSet<>(notPropagated);  // Set of tables to propagate

        // Loop over tables
        for (String table1 : propagated) {
            for (String table2 : notPropagated) {

                // Get relationships between table1 and table2
                List<ForeignConstraint> relationshipList = metaOutput.get(table1).foreignConstraintList.stream().filter(propagationForeignConstraint -> table2.equals(propagationForeignConstraint.fTable)).collect(Collectors.toList());

                // Loop over all relationships between the tables
                for (ForeignConstraint relationship : relationshipList) {
                    // Initialize
                    OutputTable table = new OutputTable(metaInput.get(table2));
                    table.name = trim(setting, table2, metaOutput.size()); // We have to make sure that outputTable name is short enough but unique (we add id)
                    table.dateBottomBounded = true;    // NOT PROPERLY USED - is a constant
                    table.propagationForeignConstraint = relationship;
                    table.propagationTable = table1;
                    table.propagationPath = getPropagationPath(metaOutput.get(table1));

                    // Identify time constraint
                    table = TemporalConstraint.find(setting, table);

                    // Make a new table
                    table.sql = SQL.propagateID(setting, table);
                    table.isSuccessfullyExecuted = Network.executeUpdate(setting.dataSource, table.sql);

                    // Get the row count
                    // NOTE: Should be conditional on isSuccessfullyPropagated (also below).
                    table.rowCount = SQL.getRowCount(setting, setting.outputSchema, table.name);
                    logger.debug("Table \"" + table.originalName + "\" with \"" + table.temporalConstraint + "\" time constraint has " + table.rowCount + " rows.");

                    // If the produced table has 0 rows and time constrain was used, repeat without any time constraint.
                    if (table.rowCount == 0 && table.temporalConstraint != null) {
                        SQL.dropTable(setting, table.name);
                        table.temporalConstraint = null;
                        table.temporalConstraintJustification = "The attempt to use time constrain failed - no row satisfies the time frame defined in the initial setting of Predictor Factory.";

                        // Make a new table
                        table.sql = SQL.propagateID(setting, table);
                        table.isSuccessfullyExecuted = Network.executeUpdate(setting.dataSource, table.sql);

                        // Get the row count
                        table.rowCount = SQL.getRowCount(setting, setting.outputSchema, table.name);
                        logger.debug("Table \"" + table.originalName + "\" with \"" + table.temporalConstraint + "\" time constraint has " + table.rowCount + " rows.");
                    }

                    // Add indexes
                    SQL.addIndex(setting, table.name);

                    // Is OK?
                    table.isOk = (table.isSuccessfullyExecuted && table.rowCount > 0);

                    // Update the state
                    stillNotPropagated.remove(table2);
                    if (table.isOk) {
                        newlyPropagated.add(table.name);
                    }

                    // Collect metadata
                    table.isTargetIdUnique = SQL.isTargetIdUnique(setting, table.name);
                    table.propagationOrder = metaOutput.size();
                    table.timestampDelivered = LocalDateTime.now();

                    // Add the table into tableMetadata list
                    metaOutput.put(table.name, table);

                    // Log it
                    SQL.addToJournalTable(setting, table);
                }
            }
        }
    
        // If we didn't propagate ANY new table (i.e. tables are truly independent of each other) or we
        // propagated all the tables or we reached the maximal propagation depth, return the conversionMap.
        if (newlyPropagated.isEmpty() || stillNotPropagated.isEmpty() || depth>=setting.propagationDepthMax) {
            // Output quality control
            if (!stillNotPropagated.isEmpty()) {
                logger.warn("Following tables were not propagated: " + stillNotPropagated.toString());
                logger.info("The maximal allowed propagation depth is: " + setting.propagationDepthMax);
            }
        
            return metaOutput;
        }
    
        // Otherwise go a level deeper.
        logger.info("#### Propagated the base table to depth: " + depth + " ####");
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
