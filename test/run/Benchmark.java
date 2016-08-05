package run;

import com.rapidminer.Process;
import com.rapidminer.RapidMiner;
import com.rapidminer.operator.IOContainer;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.performance.PerformanceVector;
import com.rapidminer.tools.XMLException;
import connection.Network;
import connection.SQL;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/* 
Loop over datasets and:
	1) Run Predictor Factory
	2) Run RapidMiner
*/ 
public class Benchmark {

	static String note = "Without id attributes. All feature functions. Bagging.";

	public static void main(String[] args) {
		// Initialization
		String[] parameterList = new String[2];
//		String[] databaseList = {
////				"Accidents",		// Time consuming. (Propagation to tocka)
////				"AdventureWorks",	// Fails on lack of free space (WorkOrder is not propagated)
//				"AustralianFootball",
////				"Basketball_women",	// Fails - a composite ID is something new
//				"Bupa",	// trivial
////				"Carcinogenesis",	// On MariaDB (FK violations)
////				"CORA",		// Results in empty mainSample!
////				"Credit",	// Time consuming (32 minutes)
//				"cs",
////				"cs_large",   	// Have to check the dataset
//				"DCG",
//				"Dunur",
//				"Elti",
//				"financial",
//				"Financial_std",
//				"ftp",
//				"genes",
//				"genes_sym",
//				"Hepatitis_std",
////				"imdb_ijs", 	// 3 hours. Out of memory during classification.
////				"imdb_MovieLens",	// Time consuming (24 minutes)
//				"KRK",
////				"legalActs",	// 20 minutes. Out of memory during modeling.
////				"Mondial",		// Works, although uses combined constrains
//				"Mondial_geo",
//				"MuskLarge",
//				"MuskSmall",
//				"mutagenesis",
//				"mutagenesis_disc_sym",
//				"mutagenesis_42",
//				"mutagenesis_188",
//				"mutagenesis_disc",
////				"nations",	// Time consuming because of a wide table and ratio pattern (4 minutes)
//				"NBA",
//				"NBA_sym",
////				"NCAA",		// Time consuming (10 minutes)
//				"northwind",
//				"Pima",
////				"PremierLeague", // 5 minutes
//				"PTE",	// Easy
//				"pubs",
//				"sakila",
//				"Same_gen",	// Easy
////				"stats",	// Troublesome. Big?
////				"Toxicology",	// On MariaDB (FK violations)
//				"trains",
//				"unielwin",
//				"UW_std",
//				"world"
//};

		String[] databaseList = {
//				"Accidents",		// Time consuming. (Propagation to tocka)
//				"AdventureWorks",	// Fails on lack of free space (WorkOrder is not propagated)

//				"Carcinogenesis",	// On MariaDB (FK violations)
				"cs",
				"imdb_MovieLens",	// Time consuming (24 minutes)
				"Mondial",		// Works, although uses combined constrains
				"Mondial_geo",
				"NCAA",		// Time consuming (10 minutes)
				"PremierLeague", // 5 minutes
//				"Toxicology",	// On MariaDB (FK violations)
		};

		// Do not print errors printed into System.error (like from RapidMiner)
		PrintStream streamBkp = System.err;
		System.setErr(new PrintStream(new OutputStream() {
			public void write(int b) {
			}
		}));

		// RapidMiner initialization
		System.out.println("Initialising RapidMiner...");
		RapidMiner.setExecutionMode(RapidMiner.ExecutionMode.COMMAND_LINE);
		RapidMiner.init();

		// Restore printing of errors
		System.setErr(streamBkp);

		// Loop
		for (String database : databaseList) {
			// Setting		
			parameterList[0] = "PostgreSQL";	// Classification will fail with MariaDB - the mainsample will be on the wrong DB.
			parameterList[1] = database;
			
			// Execute Predictor Factory
			Launcher.main(parameterList);

			// Open the database connection with the correct setting
			Setting setting = new Setting("PostgreSQL", database);
			setting = Network.openConnection(setting);

			// Execute RapidMiner - evaluate predictive power with cross-validation.
			System.out.println("Learning and evaluating a model...");
			if ("regression".equals(setting.task)) {
				regression(setting);
			} else {
				List<String> uniqueRecords = SQL.getUniqueRecords(setting, setting.targetTable, setting.targetColumn, true);

				if (uniqueRecords.size() == 2) binaryClassification(setting);
				else classification(setting);
			}

			// Close the connection
			Network.closeConnection(setting);
		}

		RapidMiner.quit(RapidMiner.ExitMode.NORMAL);
	}


	// Subroutines: Perform modeling on mainSample, which resides in a database.
	// The performance on the dataset is appended into the log in the database.
	private static void regression(Setting setting) {
		double correlation = -1;
		double correlation_std = -1;
		double rmse = -1;
		double rmse_std = -1;
		double relative_error = -1;
		double relative_error_std = -1;

		try {
			Process process = new Process(new File("/Users/jan/.RapidMiner5/repositories/Local Repository/processes/Predictor Factory/PostgreSQL - just regression.rmp"));
			IOContainer computed = process.run();
			correlation = computed.get(PerformanceVector.class).getCriterion("correlation").getAverage();
			correlation_std = computed.get(PerformanceVector.class).getCriterion("correlation").getStandardDeviation();
			relative_error = computed.get(PerformanceVector.class).getCriterion("relative_error").getAverage();
			relative_error_std = computed.get(PerformanceVector.class).getCriterion("relative_error").getStandardDeviation();
			rmse = computed.get(PerformanceVector.class).getCriterion("root_mean_squared_error").getAverage();
			rmse_std = computed.get(PerformanceVector.class).getCriterion("root_mean_squared_error").getStandardDeviation();
		} catch (IOException | XMLException | OperatorException ex) {
			ex.printStackTrace();
		}

		//Network.executeUpdate(setting.dataSource, "UPDATE " + setting.outputSchema + ".log SET note='" + note + "', correlation=" + correlation + ", correlation_std=" + correlation_std +  ", rmse=" + rmse + ", rmse_std=" + rmse_std + ", relative_error=" + relative_error + ", relative_error_std=" + relative_error_std + ", \"timestamp\" = now(), predictor_count=b.predictor_count, propagated_table_count=c.propagated_table_count from (select max(id) max_id from " + setting.outputSchema + ".log) a, (select count(*) predictor_count from " + setting.outputSchema + ".journal) b, (select count(*) propagated_table_count from information_schema.tables where table_schema = 'predictor_factory' and \"table_name\" like 'propagated_%') c WHERE id = a.max_id ");

		// Upload the result (note that RapidMiner may return NaNs)
		String statement = "UPDATE " + setting.outputSchema + ".log SET setting='" + setting + "', note='" + note + "', correlation=?, correlation_std=?, rmse=?, rmse_std=?, relative_error=?, relative_error_std=?, \"timestamp\" = now(), predictor_count=b.predictor_count, propagated_table_count=c.propagated_table_count from (select max(id) max_id from " + setting.outputSchema + ".log) a, (select count(*) predictor_count from " + setting.outputSchema + ".journal) b, (select count(*) propagated_table_count from information_schema.tables where table_schema = 'predictor_factory' and \"table_name\" like 'propagated_%') c WHERE id = a.max_id ";

		try (Connection connection = setting.dataSource.getConnection();
			 PreparedStatement preparedStatement = connection.prepareStatement(statement)){
			preparedStatement.setDouble(1, correlation);
			preparedStatement.setDouble(2, correlation_std);
			preparedStatement.setDouble(3, rmse);
			preparedStatement.setDouble(4, rmse_std);
			preparedStatement.setDouble(5, relative_error);
			preparedStatement.setDouble(6, relative_error_std);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void binaryClassification(Setting setting) {
		Double accuracy = -1.0;
		double accuracy_std = -1;
		double auc = -1;
		double auc_std = -1;
		double auc_optimistic = -1;
		double auc_optimistic_std = -1;
		double auc_pessimistic = -1;
		double auc_pessimistic_std = -1;
		double auc_average = -1;
		double auc_average_std = -1;
		double fscore = -1;
		double fscore_std = -1;
		double precision = -1;
		double precision_std = -1;
		double recall = -1;
		double recall_std = -1;

		try {
			Process process = new Process(new File("/Users/jan/.RapidMiner5/repositories/Local Repository/processes/Predictor Factory/PostgreSQL - binominal classification.rmp"));
			IOContainer computed = process.run();
			accuracy = computed.get(PerformanceVector.class).getCriterion("accuracy").getAverage();
			accuracy_std = computed.get(PerformanceVector.class).getCriterion("accuracy").getStandardDeviation();
			auc = computed.get(PerformanceVector.class).getCriterion("AUC").getAverage();
			auc_std = computed.get(PerformanceVector.class).getCriterion("AUC").getStandardDeviation();
			auc_optimistic = computed.get(PerformanceVector.class).getCriterion("AUC (optimistic)").getAverage();
			auc_optimistic_std = computed.get(PerformanceVector.class).getCriterion("AUC (optimistic)").getStandardDeviation();
			auc_pessimistic = computed.get(PerformanceVector.class).getCriterion("AUC (pessimistic)").getAverage();
			auc_pessimistic_std = computed.get(PerformanceVector.class).getCriterion("AUC (pessimistic)").getStandardDeviation();
			auc_average = (auc_optimistic + auc_pessimistic)/2.0;
			auc_average_std = (auc_optimistic_std + auc_pessimistic_std)/2.0;
			fscore = computed.get(PerformanceVector.class).getCriterion("f_measure").getAverage();
			fscore_std = computed.get(PerformanceVector.class).getCriterion("f_measure").getStandardDeviation();
			precision = computed.get(PerformanceVector.class).getCriterion("precision").getAverage();
			precision_std = computed.get(PerformanceVector.class).getCriterion("precision").getStandardDeviation();
			recall = computed.get(PerformanceVector.class).getCriterion("recall").getAverage();
			recall_std = computed.get(PerformanceVector.class).getCriterion("recall").getStandardDeviation();
		} catch (IOException | XMLException | OperatorException ex) {
			ex.printStackTrace();
		}

		//Network.executeUpdate(setting.dataSource, "UPDATE " + setting.outputSchema + ".log SET note='" + note + "', accuracy=" + accuracy + ", accuracy_std=" + accuracy_std + ", auc=" + auc + ", auc_std=" + auc_std + ", auc_optimistic=" + auc_optimistic + ", auc_optimistic_std=" + auc_optimistic_std + ", auc_pessimistic=" + auc_pessimistic + ", auc_pessimistic_std=" + auc_pessimistic_std + ", auc_average=" + auc_average + ", auc_average_std=" + auc_average_std + ", fscore=" + fscore + ", fscore_std=" + fscore_std + ", precision=" + precision + ", precision_std=" + precision_std + ", recall=" + recall + ", recall_std=" + recall_std + ", \"timestamp\" = now(), predictor_count=b.predictor_count, propagated_table_count=c.propagated_table_count from (select max(id) max_id from " + setting.outputSchema + ".log) a, (select count(*) predictor_count from " + setting.outputSchema + ".journal) b, (select count(*) propagated_table_count from information_schema.tables where table_schema = 'predictor_factory' and \"table_name\" like 'propagated_%') c WHERE id = a.max_id ");

		// Upload the result (note that RapidMiner may return NaNs)
		String statement = "UPDATE " + setting.outputSchema + ".log SET setting='\" + setting + \"', note='" + note + "', accuracy=?, accuracy_std=?, auc=?, auc_std=?, auc_optimistic=?, auc_optimistic_std=?, auc_pessimistic=?, auc_pessimistic_std=?, auc_average=?, auc_average_std=?, fscore=?, fscore_std=?, precision=?, precision_std=?, recall=?, recall_std=?, \"timestamp\" = now(), predictor_count=b.predictor_count, propagated_table_count=c.propagated_table_count from (select max(id) max_id from " + setting.outputSchema + ".log) a, (select count(*) predictor_count from " + setting.outputSchema + ".journal) b, (select count(*) propagated_table_count from information_schema.tables where table_schema = 'predictor_factory' and \"table_name\" like 'propagated_%') c WHERE id = a.max_id ";

		try (Connection connection = setting.dataSource.getConnection();
			 PreparedStatement preparedStatement = connection.prepareStatement(statement)){
			preparedStatement.setDouble(1, accuracy);
			preparedStatement.setDouble(2, accuracy_std);
			preparedStatement.setDouble(3, auc);
			preparedStatement.setDouble(4, auc_std);
			preparedStatement.setDouble(5, auc_optimistic);
			preparedStatement.setDouble(6, auc_optimistic_std);
			preparedStatement.setDouble(7, auc_pessimistic);
			preparedStatement.setDouble(8, auc_pessimistic_std);
			preparedStatement.setDouble(9, auc_average);
			preparedStatement.setDouble(10, auc_average_std);
			preparedStatement.setDouble(11, fscore);
			preparedStatement.setDouble(12, fscore_std);
			preparedStatement.setDouble(13, precision);
			preparedStatement.setDouble(14, precision_std);
			preparedStatement.setDouble(15, recall);
			preparedStatement.setDouble(16, recall_std);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static void classification(Setting setting) {
		double accuracy = -1;
		double accuracy_std = -1;
		
		try {
			Process process = new Process(new File("/Users/jan/.RapidMiner5/repositories/Local Repository/processes/Predictor Factory/PostgreSQL - just accuracy.rmp"));
			IOContainer computed = process.run();
			accuracy = computed.get(PerformanceVector.class).getCriterion("accuracy").getAverage();
			accuracy_std = computed.get(PerformanceVector.class).getCriterion("accuracy").getStandardDeviation();
		} catch (IOException | XMLException | OperatorException ex) {
			ex.printStackTrace();
		}

		// Upload the result (note that RapidMiner may return NaNs)
		String statement = "update " + setting.outputSchema + ".log SET setting='\" + setting + \"', note='" + note + "', accuracy=?, accuracy_std=?, \"timestamp\" = now(), predictor_count=b.predictor_count, propagated_table_count=c.propagated_table_count from (select max(id) max_id from " + setting.outputSchema + ".log) a, (select count(*) predictor_count from " + setting.outputSchema + ".journal) b, (select count(*) propagated_table_count from information_schema.tables where table_schema = 'predictor_factory' and \"table_name\" like 'propagated_%') c WHERE id = a.max_id ";

		try (Connection connection = setting.dataSource.getConnection();
			 PreparedStatement preparedStatement = connection.prepareStatement(statement)){
			preparedStatement.setDouble(1, accuracy);
			preparedStatement.setDouble(2, accuracy_std);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
