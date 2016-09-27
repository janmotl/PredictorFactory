package metaInformation;

import connection.SQL;
import org.apache.log4j.Logger;
import run.Setting;
import utility.BlackWhiteList;
import utility.Meta;
import utility.Text;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


public class MetaInput {

    // Logging
    private static final Logger logger = Logger.getLogger(MetaInput.class.getName());


    // Return map of tables in the input schema
    // IDS ARE WRONGFULLY IGNORED FROM THE BLACK/WHITE LISTS
    public static SortedMap<String, Table> getMetaInput(Setting setting) {
    
        // Initialization
        SortedMap<String, Table> tableMap;                                          // Map of {tableName, tableData}
        final String database = setting.database;
        final String schema = setting.inputSchema;
        final List<String> whiteListTable = Text.string2list(setting.whiteListTable); // Parsed values
        final List<String> blackListTable = Text.string2list(setting.blackListTable); // Parsed values
        final Map<String,List<String>> whiteMapColumn = Text.list2map(Text.string2list(setting.whiteListColumn)); // Parsed values
        final Map<String,List<String>> blackMapColumn = Text.list2map(Text.string2list(setting.blackListColumn)); // Parsed values
    
        // What data types are used by the database? In journal file we use {3, 4, 12 and 93}.
        logger.debug("Supported data types are: " + getDataTypes(setting));

        // QC input and output schemas
        SortedSet<String> schemaSet = Meta.collectSchemas(setting, setting.database);
    
        if (setting.inputSchema != null && !schemaSet.contains(setting.inputSchema)) {
            logger.warn("The input schema '" + setting.inputSchema + "' doesn't exist in the database.");
            logger.warn("Available schemas in the database are: " + schemaSet);
        }
        if (setting.outputSchema != null && !schemaSet.contains(setting.outputSchema)) {
            logger.warn("The output schema '" + setting.outputSchema + "' doesn't exist in the database.");
            logger.warn("Available schemas in the database are: " + schemaSet);
        }
    
            
        // Get tables
        tableMap = utility.Meta.collectTables(setting, database, schema);
    
        // QC that targetTable is in the tableSet
        if (!tableMap.keySet().contains(setting.targetTable)) {
            logger.warn("The target table '" + setting.targetTable + "' doesn't exist in the database.");
            logger.warn("Available tables in the database are: " + tableMap.keySet());
        }

        // QC that targetDate doesn't contain nulls
        // Note: we do not use Column call because Columns are still not populated
        if (setting.targetDate != null) {
            if (SQL.containsNull(setting, setting.targetTable, setting.targetDate)) {
                logger.warn("Target date column '" + setting.targetDate + "' contains null. Rows with the null in target date column WILL BE IGNORED!");
            }
        }

        // Apply black/white lists
        tableMap = BlackWhiteList.filter(tableMap, blackListTable, whiteListTable);
    
        // QC that count of tables after blacklisting is > 0
        if (tableMap.isEmpty()) {
            logger.warn("The count of available tables is 0.");
        }

        // QC that targetTable exists and we can use it
        if (!tableMap.containsKey(setting.targetTable)) {
            logger.warn("The target table is not among permitted tables. Check your blacklist/whitelist.");
        }



        // Collect columns and relationships
        // I could have collected all the metadata at schema level. The runtime would be faster,
        // because just one query would be performed (instead of making a unique query for each table).
        // But implementation would be slightly more complex.
        for (Table table : tableMap.values()) {
        
            // Collect columns
            table.columnMap = Meta.collectColumns(setting, database, schema, table.name);

            // Respect white/black lists
            table.columnMap = BlackWhiteList.filter(table.columnMap, blackMapColumn.get(table.name), whiteMapColumn.get(table.name));

            // Store relationships
            table.foreignConstraintList = Meta.collectRelationships(setting, schema, table.name);
        
            // Identify ids based on FK
            table.identifyId();
        
            // Identify artificial ids based on PK (PKs may not participate in any relationship)
            String pk = Meta.getPrimaryKey(setting, database, schema, table.name);
            if (pk!=null && table.columnMap.containsKey(pk)) {
                table.getColumn(pk).isId = true;
                table.getColumn(pk).isNullable = false;
                table.getColumn(pk).isUnique = true;
            }
        
            // Store numericalColumn, nominalColumn and timeColumn
            table.categorizeColumns(setting);

            // Get the most frequent unique values for each nominal attribute.
            // For target get all unique values.
            // And set setting.isTargetString
            // NOTE: This is ugly. The function should do just one thing.
            table.addUniqueValues(setting);

        }

        // Do not use targetColumn as an input attribute if we work with a static dataset.
        // But use the targetColumn in dynamic datasets.
        if (setting.targetDate == null) {
            tableMap.get(setting.targetTable).getColumn(setting.targetColumn).isNominal = false;
            tableMap.get(setting.targetTable).getColumn(setting.targetColumn).isNumerical = false;
            tableMap.get(setting.targetTable).getColumn(setting.targetColumn).isTemporal = false;
        }

        // QC that TargetDate is temporal
        if (setting.targetDate != null) {
            Column column = tableMap.get(setting.targetTable).getColumn(setting.targetDate);
            if (!column.isTemporal) {
                logger.warn("Target date column '" + setting.targetDate + "' is " + column.dataTypeName + ". But a temporal data type was expected.");
            }
        }

        // QC that label is numerical if performing regression
        if ("regression".equals(setting.task)) {
            Column column = tableMap.get(setting.targetTable).getColumn(setting.targetColumn);
            if (!column.isNumerical) {
                logger.warn("Target column '" + setting.targetColumn + "' is " + column.dataTypeName + ". But a numerical data type was expected.");
            }
        }
    
        // QC that relationships were extracted
        int relationshipCount = 0;
        for (Table table : tableMap.values()) {
            relationshipCount += table.foreignConstraintList.size();
        }
        if (relationshipCount == 0) {
            logger.warn("No relationships were detected. You may either define Foreign Key Constrains in the database. Or you may create and put foreignConstraint.xml into config directory (note: entity names in the xml must exactly match the entity names in the database, EVEN if the database is case insensitive, because comparisons are performed locally, not at the server).");
        }
    
        return tableMap;
    }



//  public static boolean isSymmetrical(){
//      for (String table : tableSet) {
//          // Get relationships
//          List<List<String>> relationshipList = MetaInput.downloadRelationships(setting, setting.database, setting.inputSchema, table);
//          System.out.println(relationshipList);
//      
//          // Detection
//          String lagFTable = "";
//          String lagColumn = "";
//          for (List<String> relationship : relationshipList) {
//              if (relationship.get(0).equals(lagFTable) && relationship.get(1).equals(lagColumn)) {
//                  System.out.println("bingo " + lagFTable);
//              }
//              lagFTable = relationship.get(0);
//              lagColumn = relationship.get(1);
//          }
//      }
//  }


    // 2) Get list of all supported data types from the database
    private static List<List<String>> getDataTypes(Setting setting) {
    
        // Initialization
        List<List<String>> dataTypeList = new ArrayList<>();

        try (Connection connection = setting.dataSource.getConnection();
             ResultSet rs = connection.getMetaData().getTypeInfo()){

            while (rs.next()) {
                ArrayList<String> dataType = new ArrayList<>();
                dataType.add(rs.getString("TYPE_NAME"));
                dataType.add(rs.getString("DATA_TYPE"));
                dataTypeList.add(dataType);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        
        return dataTypeList;
    }

}
