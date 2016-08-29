/**
 * The main class of Predictor Factory.
 */
package run;

import connection.Network;
import connection.SQL;
import featureExtraction.Journal;
import metaInformation.MetaOutput.OutputTable;
import metaInformation.Table;
import org.apache.log4j.Logger;
import propagation.Propagation;
import utility.Logging;
import utility.Memory;

import java.util.SortedMap;


public class Launcher{
	// Logging
	private static final Logger logger = Logger.getLogger(Launcher.class.getName());
	

		
	// Where everything starts and ends...
	public static void main(String[] arg){

		// Connect to the following server and database:
		String connectionProperty = "SAS";	// Host identification as specified in resources/connection.xml
		String databaseProperty = "SAS";		// Dataset identification as specified in resources/database.xml

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

		// Setup logging - load the property file
		Logging.initialization();

		// Validate all XMLs
		InputQualityControl.validateConfiguration();

		// Check the system
		SystemQualityControl.validateSystem();

		// Construct the setting object
		Setting setting = new Setting(connectionProperty, databaseProperty);
		logger.info("#### Predictor Factory was initialized ####");

		// Connect to the server
		setting = Network.openConnection(setting);
		logger.debug(setting.toString());

		// Collect information about tables, columns and relations in the database
		SortedMap<String, Table> metaInput = metaInformation.MetaInput.getMetaInput(setting);
		logger.info("#### Collected metadata about the database ####");

		// Remove all the tables from the previous run
		SQL.tidyUp(setting);
		logger.info("#### Cleaned the output schema ####");

		// Setup journal of propagated tables
		SQL.getJournalTable(setting);


		// Make base table
		SQL.getBase(setting);
		if ("classification".equals(setting.task)) {
			SQL.getSubSampleClassification(setting, metaInput);
		} else {
			SQL.getSubSampleRegression(setting);
		}

		// Propagate base table
		SortedMap<String, OutputTable> outputMeta = Propagation.propagateBase(setting, metaInput);
		logger.info("#### Propagated the base table into " + outputMeta.size() +  " tables in total ####");

		// Calculate features
		Journal journal = featureExtraction.Aggregation.aggregate(setting, outputMeta);

		// Make MainSample
		SQL.getMainSample(setting, journal.getTopPredictors());
		logger.info("#### Produced " + setting.outputSchema + "." + setting.mainTable + " with " + journal.getTopPredictors().size() + " most predictive predictors from " + journal.size() + " evaluated. Duplicate or unsuccessfully calculated predictors are not passed into the output table. ####");

		// Write the status into "log" table
		long stopTime = System.currentTimeMillis();
	    long elapsedTime = stopTime - startTime;
	    SQL.logRunTime(setting, elapsedTime);

		// Be nice toward the server and close the connection
		Network.closeConnection(setting);

		// Tell us how greedy you are
		Memory.logMemoryInfo();

		// Tell the user we are finished
		logger.info("#### Finished ####");
	}
}


