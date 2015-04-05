package run;

import java.io.File;
import java.io.IOException;

import com.rapidminer.Process;
import com.rapidminer.RapidMiner;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.tools.XMLException;
import com.rapidminer.operator.performance.PerformanceVector;

public class Benchmark {

	public static void main(String[] args) {
		// Initialization
		String[] parameterList = new String[2];
		String[] databaseList = {
				"pubs",
				"northwind",
				
//				"cs",
//				"trains",
//				"imdb_ijs", 	// 3 hours. Out of memory during classification.
//				"AdventureWorks",	// Time consuming
//				"cs_large",   	// Have to check the dataset
//				"mutagenesis",
//				"mutagenesis_42",
//				"mutagenesis_188",	
//				"financial",
//				"Financial_std",
//				"genes",
//				"Hepatitis_std",
//		
//				"NBA",
//				"unielwin",
//				"UW_std",
//				"world",
//				"Mondial",		// Warning: combined constrains
//				"Mondial_geo",
//				"nations",	// Time consuming because of a wide table and ratio pattern (4 minutes!!!)
//				"NCAA",		// Time consuming (10 minutes!!!)	
//				"PremierLeague",	
//				"imdb_MovieLens",	// Time consuming
};
		
		// RapidMiner initialization
		RapidMiner.setExecutionMode(RapidMiner.ExecutionMode.COMMAND_LINE);
		RapidMiner.init();
		
		
		
		// Loop
		for (String database : databaseList) {
			// Setting		
			parameterList[0] = "PostgreSQL";
			parameterList[1] = database;
			
			// Execute Predictor Factory
			Launcher.main(parameterList);
			
			// Execute RapidMiner - evaluate predictive power with cross-validation.
			double accuracy = executeRapidMiner();
			
			// Write the accuracy into the database
			Setting setting = new Setting();
			setting = connection.Network.getConnection(setting, "PostgreSQL", "mutagenesis");
			connection.Network.executeUpdate(setting.connection, "UPDATE " + setting.outputSchema + ".log SET accuracy=" + accuracy + ", \"timestamp\" = now(), predictor_count=b.predictor_count, propagated_table_count=c.propagated_table_count from (select max(id) max_id from " + setting.outputSchema + ".log) a, (select count(*) predictor_count from " + setting.outputSchema + ".journal) b, (select count(*) propagated_table_count from information_schema.tables where table_schema = 'predictor_factory' and \"table_name\" like 'propagated_%') c WHERE id = a.max_id ");
			connection.Network.closeConnection(setting);
		}
		
		RapidMiner.quit(RapidMiner.ExitMode.NORMAL);
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