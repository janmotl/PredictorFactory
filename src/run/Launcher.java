/**
 * The main class of Predictor Factory.
 */
package run;

import connection.Network;
import extraction.Aggregation;
import extraction.Journal;
import meta.MetaInput;
import meta.MetaOutput.OutputTable;
import meta.Table;
import org.apache.log4j.Logger;
import propagation.Propagation;
import utility.Logging;
import utility.SystemQualityControl;

import java.util.SortedMap;


public class Launcher {
	// Logging
	private static final Logger logger = Logger.getLogger(Launcher.class.getName());


	// Where everything starts and ends...
	public static void main(String[] arg) {

		// Connect to the following server and database:
		String connectionProperty = "PostgreSQL";   // Host identification as specified in resources/connection.xml
		String databaseProperty = "mutagenesis";       // Dataset identification as specified in resources/database.xml

		// Read command line parameters iff they are present (and overwrite the defaults).
		if (arg.length == 1 || arg.length > 2) {
			throw new IllegalArgumentException("Exactly two arguments are expected: connectionName databaseName");
		}
		if (arg.length == 2) {
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

		// Collect information about tables, columns and relations in the database
		SortedMap<String, Table> metaInput = MetaInput.getMetaInput(setting);
		logger.info("#### Collected metadata about the database ####");

		// Remove all the tables from the previous run
		setting.dialect.prepareOutputSchema(setting);
		logger.info("#### Prepared the output schema ####");

		// Make base table
		setting.dialect.getBase(setting);
		if ("classification".equals(setting.task)) {
			setting.dialect.getSubSampleClassification(setting, metaInput);
		} else {
			setting.dialect.getSubSampleRegression(setting);
		}

		// Propagate base table
		SortedMap<String, OutputTable> outputMeta = Propagation.propagateBase(setting, metaInput);
		logger.info("#### Propagated the base table into " + outputMeta.size() + " tables in total ####");

		// Calculate features
		Journal journal = Aggregation.run(setting, outputMeta);

		// Two-stage processing
		if (setting.useTwoStages && setting.sampleCount<Integer.MAX_VALUE) {
			Journal.marshall(journal);
			TwoStages.setExploitationPhase(setting, databaseProperty, journal);
			logger.info("Evaluated predictor count: " + journal.getEvaluationCount());
			logger.info("Preserved predictor count: " + journal.getAllTopPredictors().size());
			logger.info("The exploration stage finished. Starting the exploitation phase.");
			main(new String[]{connectionProperty, "exploitationStage"});

			return;
			// We should write into journal_run table & close the connection
		}


		// Make MainSample
		setting.dialect.getAllMainSamples(setting, journal);

		// Write the status into journal_run table
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		setting.dialect.addJournalRun(setting, elapsedTime);

		// Be nice toward the server and close the connection
		Network.closeConnection(setting);

		// Tell the user we are finished
		logger.info("#### Finished ####");
	}
}


