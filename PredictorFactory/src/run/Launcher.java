package run;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import run.Metadata.Table;
import utility.Network;
import utility.SQL;
import utility.XML;


public class Launcher{
	

	// Subroutine: Return only ID columns
	// GET RID OF IT
	public static ArrayList<String> getIDColumnList(ArrayList<String> columnList){
		ArrayList<String> result = new ArrayList<String>(); 
		for (String columnName : columnList) {
			String columnNameCI = columnName.toUpperCase(Locale.ROOT);	// Case insensitive search.
			if (columnNameCI.startsWith("ID") || columnNameCI.endsWith("ID") || columnNameCI.endsWith("_NO") ) result.add(columnName);
		}
		return result;
	}
	
	public static void main(String[] arg){

		// Test that we are here
		System.out.println("#### Predictor Factory was itialized ####");
		
		// Database setting...
		Setting setting = new Setting();
				
		setting.dbType = "MySQL";
		setting.inputSchema = "financial";
		setting.outputSchema = "financial";
		
		setting.idColumn = "account_id";
		setting.idTable = "account";
		setting.targetDate = "date";
		setting.targetColumn = "status";
		setting.targetTable = "loan";
		
		setting.blackList = "('result')";

		// ...Predictor Factory setting
		setting.baseId = "propagated_id";
		setting.baseDate = "propagated_date";
		setting.baseTarget = "propagated_target";
		setting.baseTable = "base";
		
		setting.sampleTable = "mainSample";
		setting.journalTable = "journal";
		setting.statementTable = "statement";
		
		setting.propagatedPrefix = "propagated_";
		setting.predictorPrefix = "predictor";

		// Connect to the server
		setting = Network.getConnection(setting);
		
		// Remove the tables from the previous run
		SQL.tidyUp(setting);
		System.out.println("#### Finished cleaning ####");
		
		// Setup statement journal (Not currently used)
		SQL.getStatementJournal(setting);
		
		// Setup predictor journal
		Journal journal = new Journal(setting);
		
		// Make base table
		Network.executeUpdate(setting.connection, SQL.getBase(setting));
		Network.executeUpdate(setting.connection, SQL.getIndex(setting, setting.baseTable));
	
		// Propagate base table
		Map<String, String> tableList = Propagation.propagateBase(setting);
		System.out.println("#### Finished base propagation in " + journal.getRunTime() + " miliseconds ####");
		
		// Get columns in the propagated tables
		// SHOULD PASS tableList 
		Map<String, Table> metadata = Metadata.getMetadata(setting);
		System.out.println("#### Finished metadata collection in " + journal.getRunTime() + " miliseconds ####");
		
		// Read a pattern
		Pattern pattern = XML.xml2pattern("src/pattern/aggregate.xml");	// SHOULD ITERATE OVER ALL PATTERNS IN THE DIRECTORY
		String[] parameterList = pattern.getParameterMap().get("@aggregateFunction").split(",");  // CHEATING
		boolean patternCardinality = pattern.getCardinality().equals("n");	// Pattern cardinality is treated as binary
		
		// For each propagated table		
		for (String workingTable : metadata.keySet()) {
			// Skip tables with wrong cardinality
			boolean tableCardinality = metadata.get(workingTable).cardinality>1;
			if (patternCardinality != tableCardinality) {
				continue;
			}
			
			// For each numerical column
			for (String columnName : metadata.get(workingTable).numericalColumn) {		// CHEATING - should read from XML

				// For each parameter													// CHEATING
				for (String parameter : parameterList) {
					
					// Assembly the predictor
					Predictor predictor = new Predictor(pattern);
					predictor.inputTable = workingTable;
					predictor.inputTableOriginal = tableList.get(workingTable);
					SortedMap<String,	String> columnMap = new TreeMap<String, String>();		
					columnMap.put("@numericalColumn", columnName);						// CHEATING - should read from XML
					predictor.columnMap = columnMap;
					predictor.parameterList.put("@aggregateFunction", parameter);		// CHEATING
					predictor.setId(journal.getNextId(setting)); 	
					predictor.outputTable = "predictor" + (predictor.getId());
					
					// Calculate the predictor
					journal = getPredictor(setting, journal, predictor);	
				}		
			}
		}
		
		
		/////////////////////////// COPY & PASTE UGLINES /////////////////////////
		// Read a pattern
		pattern = XML.xml2pattern("src/pattern/direct_field.xml");
		patternCardinality = pattern.getCardinality().equals("n");	// Pattern cardinality is treated as binary
		
		// For each propagated table		
		for (String workingTable : metadata.keySet()) {
			// Skip tables with wrong cardinality
			boolean tableCardinality = metadata.get(workingTable).cardinality>1;
			if (patternCardinality != tableCardinality) {
				continue;
			}
			
			// For each data column
			for (String columnName : metadata.get(workingTable).dataColumn) {		// CHEATING
				
					// Assembly the predictor
					Predictor predictor = new Predictor(pattern);
					predictor.inputTable = workingTable;
					predictor.inputTableOriginal = tableList.get(workingTable);
					SortedMap<String,	String> columnMap = new TreeMap<String, String>();		
					columnMap.put("@anyColumn", columnName);						// CHEATING
					predictor.columnMap = columnMap;
					predictor.setId(journal.getNextId(setting)); 	
					predictor.outputTable = "predictor" + (predictor.getId());
					
					// Calculate the predictor
					journal = getPredictor(setting, journal, predictor);					
			}
		}
		
		/////////////////////////// COPY & PASTE UGLINES /////////////////////////
		// Read a pattern
		pattern = XML.xml2pattern("src/pattern/slope.xml");
		patternCardinality = pattern.getCardinality().equals("n");	// Pattern cardinality is treated as binary
		
		// For each propagated table
		for (String workingTable : metadata.keySet()) {
			// Skip tables with wrong cardinality
			boolean tableCardinality = metadata.get(workingTable).cardinality > 1;
			if (patternCardinality != tableCardinality) {
				continue;
			}

			// For each date column
			for (String dateName : metadata.get(workingTable).dateColumn) { // CHEATING
				
				if (dateName == null) {
					continue;
				}
				
				// For each data column
				for (String columnName : metadata.get(workingTable).numericalColumn) { // CHEATING
	
					// Assembly the predictor
					Predictor predictor = new Predictor(pattern);
					predictor.inputTable = workingTable;
					predictor.inputTableOriginal = tableList.get(workingTable);
					SortedMap<String, String> columnMap = new TreeMap<String, String>();
					columnMap.put("@numericalColumn", columnName); // CHEATING
					columnMap.put("@dateColumn", dateName); // CHEATING
					predictor.columnMap = columnMap;
					predictor.setId(journal.getNextId(setting));
					predictor.outputTable = "predictor" + (predictor.getId());
	
					// Calculate the predictor
					journal = getPredictor(setting, journal, predictor);
				}
			}
		}
		
		
		
		
		// Make MainSample
		SQL.getMainSample(setting, journal.getAllTables(), journal.getAllColumns());

		
		// Tell the user we are finished
		System.out.println("#### Finished in " + journal.getRunTime() + " miliseconds ####");
			
	}
	
	// Subroutine: create predictor with index and QC
	private static Journal getPredictor(Setting setting, Journal journal, Predictor predictor) {
		// Convert pattern to SQL
		predictor.setSql(SQL.getPredictor(setting, predictor));
		
		// Execute the SQL
		predictor.setOk(Network.executeUpdate(setting.connection, predictor.getSql()));
		
		// Add index
		// Should be conditional on success
		String sql = SQL.getIndex(setting, predictor.outputTable);
		Network.executeUpdate(setting.connection, sql);
		
		// Add row count
		sql = SQL.getRowCount(setting, predictor.outputTable);
		predictor.setRowCount(Integer.valueOf(Network.executeQuery(setting.connection, sql).get(0)));
		
		// Add univariate relevance estimate
		// Should be conditional on QC
//		Map<String, Double> relevanceList = new HashMap<String, Double>(1);
//		relevanceList.put("", SQL.getRelevance(setting, predictor.outputTable, predictor.getName()));
//		predictor.setRelevanceList(relevanceList);
		
		// Log the event into the journal
		journal.addPredictor(setting, predictor);
		
		return journal;
	}

	// Subroutine: recursively generate each possible combination of the parameters.
	// SOME OF THE PARAMETERS SHOULD BE SET BEFORE
	// MAKE SETTING & JOURNAL GLOBAL AS THERE IS JUST ONE INSTANCE OF THEM -> THE LAUNCHER SHOULD MAKE A OBJECT
	// IN PROCESS OF DEVELOPMENT
	private static void loopParameters(Setting setting, Journal journal, Predictor predictor, Pattern pattern, SortedMap<String, String> parameterMap) {
		
		// Termination condition a: empty map
		if (parameterMap.isEmpty()) {
			journal = getPredictor(setting, journal, predictor);	// Do not set any parameter, just get the predictor.
			return;
		}
		
		// Termination condition b: map with one key
		if (parameterMap.size()==1) {
			String parameterName = parameterMap.firstKey();	
			
			// Loop over each parameter value
			for (String parameterValue : parameterMap.get(parameterName).split(",")) {
				predictor.parameterList.put(parameterName, parameterValue);	
				journal = getPredictor(setting, journal, predictor);	
			}
			
			return;
		}
		
		// Reduction step:
		String parameterName = parameterMap.firstKey();
		
		// Loop over each parameter value and add it to the predictor
		for (String parameterValue : parameterMap.get(parameterName).split(",")) {
			predictor.parameterList.put(parameterName, parameterValue);	
		}
		
		parameterMap.remove(parameterName);	// Decrement the parameterMap length by 1
		loopParameters(setting, journal, predictor, pattern, parameterMap); // Tail recursion
		
		return;
	}

}


