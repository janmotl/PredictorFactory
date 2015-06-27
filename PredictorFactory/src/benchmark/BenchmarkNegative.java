package benchmark;

import java.io.File;
import java.io.IOException;
import java.util.List;

import run.Setting;

import com.rapidminer.Process;
import com.rapidminer.RapidMiner;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.tools.XMLException;
import com.rapidminer.operator.performance.PerformanceVector;

import connection.SQL;

/*
What happens if we ignore given patterns?
Loop over mainSamples and figure it out.
 */
public class BenchmarkNegative {

	public static void main(String[] args) {

		// RapidMiner initialization
		RapidMiner.setExecutionMode(RapidMiner.ExecutionMode.COMMAND_LINE);
		RapidMiner.init();
		
		// Get metadata
		Setting setting = new Setting();
		setting = connection.Network.getConnection(setting, "PostgreSQL", "mutagenesis");
		List<String> journalList = connection.Network.executeQuery(setting.connection, "select table_name from information_schema.tables where table_schema = 'predictor_factory' and left(table_name, 2) = 'j_' and table_name > 'j_imdb_ijs' ORDER BY 1 ");
		
		
		
		
		// Loop over journals and patterns
		for (String journal : journalList) {
			
			String mainSample = "ms_" + journal.substring(2, journal.length());
			List<String> patternList = SQL.getUniqueRecords(setting, journal, "pattern_name", false); 
			
			for (String pattern : patternList) {
				
				// Get list of predictors
				List<String> predictorList = connection.Network.executeQuery(setting.connection, "select predictor_name from (select *  from predictor_factory.\"" + journal + "\" where relevance is not null and relevance > 0 order by relevance DESC limit 197) t where pattern_name <> '" + pattern + "'");
				
				// Drop mainSample
				SQL.getDropTable(setting, "mainSample");
				
				// Create new mainSample
				String sql = "create table predictor_factory.\"mainSample\" as select propagated_target, propagated_id, \"" + String.join("\", \"", predictorList) + "\" from predictor_factory.\"" + mainSample + "\"";
				connection.Network.executeUpdate(setting.connection, sql);
			
				// Execute RapidMiner - evaluate predictive power with cross-validation.
				double accuracy = executeRapidMiner();
				
				// Write the accuracy into the database
				connection.Network.executeUpdate(setting.connection, "insert into predictor_factory.log_pattern (schema_name, missing_pattern, accuracy, timestamp) values ('" + mainSample.substring(3, mainSample.length()) + "', '" + pattern + "', " + accuracy + ", now())");
				System.out.println(mainSample.substring(3, mainSample.length()) + " " + pattern + ": " + accuracy);
				
			}
			
		}
		
		// Close everything
		RapidMiner.quit(RapidMiner.ExitMode.NORMAL);
		connection.Network.closeConnection(setting);
	}
	
	// Subroutine: Perform classification on mainSample, which resides in the database.
	// The performance on the dataset is appended into a log.
	private static double executeRapidMiner() {
		double accuracy = 0;
		
		try {
			Process process = new Process(new File("/Users/jan/.RapidMiner5/repositories/Local Repository/processes/Predictor Factory/PostgreSQL - just accuracy.rmp"));
			//Process process = new Process(new File("/Users/jan/.RapidMiner5/repositories/Local Repository/processes/QuickAndDirt.rmp"));
			accuracy = process.run().get(PerformanceVector.class).getCriterion("accuracy").getAverage();
		} catch (IOException | XMLException | OperatorException ex) {
			ex.printStackTrace();
		}
		
		return accuracy;
	}

}
