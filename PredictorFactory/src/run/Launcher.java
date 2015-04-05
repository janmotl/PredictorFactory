package run;

import java.util.SortedMap;

import org.apache.log4j.Logger;

import propagation.Propagation;
import propagation.MetaOutput.OutputTable;
import utility.Meta.Table;
import connection.Network;
import connection.SQL;


public class Launcher{
	// Logging
	public static final Logger logger = Logger.getLogger(Launcher.class.getName());

	public static void main(String[] arg){
		
		logger.info("#### Predictor Factory was initialized ####");
		long startTime = System.currentTimeMillis();
		
		// Database setting
		Setting setting = new Setting();
		String connectionProperty = "Azure";	// Host identification as specified in resources/connection.xml
		String databaseProperty = "financial";		// Dataset identification as specified in resources/database.xml 
		
		// Read command line parameters if they are present (and overwrite defaults).
		if (arg.length>0) {
		 connectionProperty = arg[0];
		 databaseProperty = arg[1];
		}

		// Perform the initial control 
		InputQualityControl.validateConfiguration(setting);
		
		// Connect to the server
		setting = Network.getConnection(setting, connectionProperty, databaseProperty);
		
		// Collect information about tables, columns and relations in the database
		SortedMap<String, Table> inputMeta = metaInformation.MetaInput.getMetaInput(setting);
						
		// Remove all the tables from the previous run
		SQL.tidyUp(setting);
		logger.info("#### Finished cleaning ####");
		
		// Setup predictor journal
		Journal journal = new Journal(setting);
		
		// Make base table
		SQL.getBase(setting);
		
		// Propagate base table
		SortedMap<String, OutputTable> outputMeta = Propagation.propagateBase(setting, inputMeta);
		logger.info("#### Finished base propagation into " + outputMeta.size() +  " tables ####");
						
		// Loop over all patterns in pattern directory
		featureExtraction.Aggregation.loopPatterns(setting, journal, outputMeta);
				
		// Make MainSample
		SQL.getMainSample(setting, journal.getJournal());
		
		/////////////// Benchmark stuff /////////////
		logger.info("#### Starting the benchmark ####");
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    connection.Network.executeUpdate(setting.connection, "drop table if exists " + setting.outputSchema + ".\"ms_" + setting.inputSchema + "\"");
	    connection.Network.executeUpdate(setting.connection, "create table " + setting.outputSchema + ".\"ms_" + setting.inputSchema +"\" as select * from " + setting.outputSchema + ".\"mainSample\"");
	    connection.Network.executeUpdate(setting.connection, "drop table if exists " + setting.outputSchema + ".\"j_" + setting.inputSchema + "\"");
	    connection.Network.executeUpdate(setting.connection, "create table " + setting.outputSchema + ".\"j_" + setting.inputSchema +"\" as select * from " + setting.outputSchema + ".\"journal\"");
	    connection.Network.executeUpdate(setting.connection, "insert into " + setting.outputSchema + ".log (schema_name, runtime) values ('" + setting.inputSchema + "', " + elapsedTime + ")");
		///// END Benchmark stuff //////////
		
		// Be nice toward the server and close the connection
		Network.closeConnection(setting);
				
		// Tell the user we are finished
		logger.info("#### Finished testing " + journal.size() + " predictors ####");
	}
	
}


