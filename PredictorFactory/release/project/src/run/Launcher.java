/**
 * The main class of Predictor Factory.
 */
package run;

import connection.Network;
import connection.SQL;
import metaInformation.MetaOutput;
import metaInformation.MetaOutput.OutputTable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import propagation.Propagation;
import utility.Meta.Table;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.SortedMap;


public class Launcher{
	// Logging
	private static final Logger logger = Logger.getLogger(Launcher.class.getName());
	

		
	// Where everything starts and ends...
	public static void main(String[] arg){
		
		// Connect to the following server and database:
		String connectionProperty = "Oracle";	// Host identification as specified in resources/connection.xml
		String databaseProperty = "financial_xe";		// Dataset identification as specified in resources/database.xml
		
		// Read command line parameters if they are present (and overwrite the defaults).
		if (arg.length==1 || arg.length>2) { 
			throw new IllegalArgumentException("The valid arguments are: connectionName databaseName.");
		}
		if (arg.length==2) {
			connectionProperty = arg[0];
			databaseProperty = arg[1];
		}
		
		// Start measuring run time
		long startTime = System.currentTimeMillis();
				
		// Setup logging
		try {
			Properties p = new Properties();
		    p.load(new FileInputStream("config/log4j.properties"));
		    PropertyConfigurator.configure(p);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.out.println("The working directory is: " + System.getProperty("user.dir"));
		}
		
		// Validate all XMLs 
		InputQualityControl.validateConfiguration();
		
		// Construct the setting object
		Setting setting = new Setting(connectionProperty, databaseProperty);
		logger.info("#### Predictor Factory was initialized ####");
		logger.debug(setting.toString());
		
		
		// Connect to the server
		setting = Network.openConnection(setting);
		
		// Collect information about tables, columns and relations in the database
		SortedMap<String, Table> metaInput = metaInformation.MetaInput.getMetaInput(setting);
		logger.info("#### Finished collecting metadata ####");
		
		// Remove all the tables from the previous run
		SQL.tidyUp(setting);
		logger.info("#### Finished cleaning ####");
		
		// Setup journal - log of all predictors
		Journal journal = new Journal(setting);
		
		// Make base table
		SQL.getBase(setting);
		if ("classification".equals(setting.task)) {
			SQL.getSubSampleClassification(setting, metaInput);
		} else {
			SQL.getSubSampleRegression(setting);
		}
		
		// Propagate base table
		SortedMap<String, OutputTable> outputMeta = Propagation.propagateBase(setting, metaInput);
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


