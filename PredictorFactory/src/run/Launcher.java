/**
 * The main class of Predictor Factory.
 */
package run;

import java.util.SortedMap;

import metaInformation.MetaOutput;
import metaInformation.MetaOutput.OutputTable;

import org.apache.log4j.Logger;

import propagation.Propagation;
import utility.Meta.Table;
import connection.Network;
import connection.SQL;


public class Launcher{
	// Logging
	public static final Logger logger = Logger.getLogger(Launcher.class.getName());

	
	// Where everything starts and ends...
	public static void main(String[] arg){
		
		logger.info("#### Predictor Factory was initialized ####");
		long startTime = System.currentTimeMillis();
		
		// Database setting
		Setting setting = new Setting();
		String connectionProperty = "MariaDB";	// Host identification as specified in resources/connection.xml
		String databaseProperty = "financial";		// Dataset identification as specified in resources/database.xml 
		
		// Read command line parameters if they are present (and overwrite defaults).
		if (arg.length>2) { 
			throw new IllegalArgumentException("The valid arguments are: connectionName databaseName.");
		}
		if (arg.length>0) {
			connectionProperty = arg[0];
			databaseProperty = arg[1];
		}

		// Validate configuration 
		InputQualityControl.validateConfiguration(setting);
		
		// Connect to the server
		setting = Network.getConnection(setting, connectionProperty, databaseProperty);
		
		// Collect information about tables, columns and relations in the database
		SortedMap<String, Table> inputMeta = metaInformation.MetaInput.getMetaInput(setting);
		logger.info("#### Finished collecting metadata ####");
		
		// Remove all the tables from the previous run
		SQL.tidyUp(setting);
		logger.info("#### Finished cleaning ####");
		
		// Setup journal - log of all predictors
		Journal journal = new Journal(setting);
		
		// Make base table
		SQL.getBase(setting);
		SQL.getSubSample(setting);
		
		// Propagate base table
		SortedMap<String, OutputTable> outputMeta = Propagation.propagateBase(setting, inputMeta);
		MetaOutput.exportPropagationSQL(outputMeta);
		logger.info("#### Finished base propagation into " + outputMeta.size() +  " tables ####");
						
		// Loop over all patterns in pattern directory
		featureExtraction.Aggregation.aggregate(setting, journal, outputMeta);
				
		// Make MainSample
		SQL.getMainSample(setting, journal.getJournal());
		logger.info("#### Finished making the table of predictors ####");
		
		// Write the status into "log" table
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    SQL.logRunTime(setting, elapsedTime);
		
		// Be nice toward the server and close the connection
		Network.closeConnection(setting);
				
		// Tell the user we are finished
		logger.info("#### Finished testing " + journal.size() + " predictors ####");
	}
	
}


