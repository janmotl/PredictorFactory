package benchmark;

import com.rapidminer.Process;
import com.rapidminer.RapidMiner;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.performance.PerformanceVector;
import com.rapidminer.tools.XMLException;
import run.Launcher;
import run.Setting;

import java.io.File;
import java.io.IOException;

/* 
Loop over datasets and:
	1) Run Predictor Factory
	2) Run RapidMiner
*/ 
public class Benchmark {

	public static void main(String[] args) {
		// Initialization
		String[] parameterList = new String[2];
		String[] databaseList = {
//				"Accidents",		// Time consuming. (Propagation to tocka)
//				"AdventureWorks",	// Time consuming
//				"Carcinogenesis",	// On MariaDB (FK violations)
//				"cs",
//				"cs_large",   	// Have to check the dataset
//				"financial",
//				"Financial_std",
//				"ftp",
//				"genes",
//				"genes_sym",
//				"Hepatitis_std",
//				"imdb_ijs", 	// 3 hours. Out of memory during classification.
//				"legalActs",	// 20 minutes. Out of memory during modeling.
//				"mutagenesis",
//				"mutagenesis_disc_sym",
//				"mutagenesis_42",
//				"mutagenesis_188",
//				"mutagenesis_disc",
//				"NBA",
//				"NBA_sym",
//				"northwind",
//				"trains",
//				"unielwin",
				"UW_std",
				"world",
				"Mondial",		// Warning: combined constrains
				"Mondial_geo",
//				"Toxicology",	// On MariaDB (FK violations)
//				"nations",	// Time consuming because of a wide table and ratio pattern (4 minutes)
//				"NCAA",		// Time consuming (10 minutes)
//				"PremierLeague", // 5 minutes	
				"pubs",
//				"imdb_MovieLens",	// Time consuming (24 minutes)
//				"Credit",	// Time consuming (32 minutes)
				"sakila",
				"lahman_2014",
//				"AdventureWorks",	// Fails on lack of free space (WorkOrder is not propagated)
				"stats"
};
		
		// RapidMiner initialization
		RapidMiner.setExecutionMode(RapidMiner.ExecutionMode.COMMAND_LINE);
		RapidMiner.init();
		
		
		
		// Loop
		for (String database : databaseList) {
			// Setting		
			parameterList[0] = "PostgreSQL";	// Classification will fail with MariaDB - the mainsample will be on the wrong DB.
			parameterList[1] = database;
			
			// Execute Predictor Factory
			Launcher.main(parameterList);
			
			// Execute RapidMiner - evaluate predictive power with cross-validation.
			double accuracy = executeRapidMiner();
			
			// Write the accuracy into the database
			Setting setting = new Setting("PostgreSQL", database);
			setting = connection.Network.openConnection(setting);
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
