package run;

import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;

import org.apache.log4j.Logger;

import connection.Network;
import connection.SQL;

// Evaluate symmetry...
public class CopyOfLauncher{
	// Logging
	public static final Logger logger = Logger.getLogger(CopyOfLauncher.class.getName());

	public static void main(String[] arg){
		
		logger.info("#### Predictor Factory was initialized ####");
		
		// Database setting
		Setting setting = new Setting();
		String connectionProperty = "PostgreSQL";	// Host identification as specified in resources/connection.xml
		String databaseProperty = "cs";		// Dataset identification as specified in resources/database.xml 
		
		// Read command line parameters if they are present (and overwrite defaults).
		if (arg.length>0) {
		 connectionProperty = arg[0];
		 databaseProperty = arg[1];
		}

	
		// Connect to the server
		setting = Network.openConnection(setting, connectionProperty, databaseProperty);
		
		// Get tables
		SortedSet<String> tableSet = utility.Meta.collectTables(setting, setting.database, setting.inputSchema);
		
		for (String table : tableSet) {
			// Get relationships 
			List<List<String>> relationshipList = utility.Meta.collectRelationships(setting, setting.database, setting.inputSchema, table);
			System.out.println(relationshipList);
			
			// Detection of a loop between two tables - a sign of possible symmetry.
			// We are relying on the fact that the relations are sorted by FTable from "collectRelationships".
			String lagFTable = "";
			String lagColumn = "";
			String lagFColumn = "";
			for (List<String> relationship : relationshipList) {
				String fTable = relationship.get(0);
				String column = relationship.get(1);
				String fColumn = relationship.get(2);
				
				if (fTable.equals(lagFTable) && fColumn.equals(lagFColumn)) {
					System.out.println("bingo " + table);
					
					// Validate the symmetry
					HashMap<String, String> map = new HashMap<String, String>();
					map.put("@column", column);
					map.put("@lagColumn", lagColumn);
					map.put("@inputTable", table);
					boolean result = SQL.isSymmetric(setting, map);
					
					System.out.println(result);
					break;	// We know we have to consider the table. No need to dig further.
					
				}
				lagFTable = fTable;
				lagColumn = column;
				lagFColumn = fColumn;
			}
		}
		
		
		// Be nice toward the server and close the connection
		Network.closeConnection(setting);
				
		// Tell the user we are finished
		logger.info("#### Finished testing predictors ####");
	}
	
}


