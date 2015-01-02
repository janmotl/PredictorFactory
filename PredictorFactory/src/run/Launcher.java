package run;

import java.io.File;
import java.util.ArrayList;
import java.util.Locale;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.rits.cloning.Cloner;

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
		
//		setting.dbType = "PostgreSQL";
//		setting.inputDatabaseName = "jan";
//		setting.outputDatabaseName = "jan";
//		setting.inputSchema = "input";
//		setting.outputSchema = "output";
		
		setting.idColumn = "account_id";
		setting.idTable = "account";
		setting.targetDate = "date";
		setting.targetColumn = "status";
		setting.targetTable = "loan";
		
		setting.blackList = "('result')"; // In SQL syntax for: " AND TABLE_NAME not in " + setting.blackList
		
		// START Mutagenesis specific
//		setting.dbType = "MySQL-mutagenesis";
//		setting.inputSchema = "mutagenesis";
//		setting.outputSchema = "mutagenesis";
//		
//		setting.idTable = "molecule";
//		setting.idColumn = "molecule_id";
//		
//		setting.targetTable = "molecule";
//		setting.targetColumn = "mutagenic";
//		setting.targetDate = "molecule_id";	// This is a dataset without any date. Id is used as a placeholder.
//		
//		setting.blackList = "('mutagenesis-atoms', 'mutagenesis-bonds', 'mutagenesis-chains')";
		// END

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
		SortedMap<String, Table> tableMetadata = Propagation.propagateBase(setting);
		System.out.println("#### Finished base propagation in " + journal.getRunTime() + " miliseconds ####");
		
		// Get columns in the propagated tables
		tableMetadata = Metadata.getMetadata(setting, tableMetadata);		
		System.out.println("#### Finished metadata collection in " + journal.getRunTime() + " miliseconds ####");
		
		// Loop over all patterns in pattern directory
		loopPatterns(setting, journal, tableMetadata);
				
		// Make MainSample
		SQL.getMainSample(setting, journal.getAllTables(), journal.getAllColumns());

		// Be nice toward the server and close the connection
		try {setting.connection.close();} catch (Exception e) {}
		
		// Tell the user we are finished
		System.out.println("#### Finished in " + journal.getRunTime() + " miliseconds ####");
	}
	
	// Subroutine 5: Create predictor with index and QC.
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
		predictor.setRowCount(SQL.getRowCount(setting, predictor.outputTable));
		
		// Add null count
		predictor.setNullCount(predictor.getRowCount() - SQL.getNotNullCount(setting, predictor.outputTable, predictor.getName()));
		
		// Add univariate relevance estimate
		// Should be conditional on QC
//		Map<String, Double> relevanceList = new HashMap<String, Double>(1);
//		relevanceList.put("", SQL.getRelevance(setting, predictor.outputTable, predictor.getName()));
//		predictor.setRelevanceList(relevanceList);
		
		// Log the event into the journal
		journal.addPredictor(setting, predictor);
		
		return journal;
	}

	// Subroutine 4: Recursively generate each possible combination of the parameters.
	// MAKE SETTING & JOURNAL GLOBAL AS THERE IS JUST ONE INSTANCE OF THEM -> THE LAUNCHER SHOULD MAKE A OBJECT
	private static void loopParameters(Setting setting, Journal journal, Predictor predictor, SortedMap<String, String> parameterMap) {
		
		// Termination condition: empty parameterMap - do not set any parameter, just get the predictor
		if (parameterMap.isEmpty()) {
			// Assembly the predictor
			predictor.setId(journal.getNextId(setting)); 	
			predictor.outputTable = "predictor" + (predictor.getId());
			
			// Get an immutable copy of the predictor and store it into the journal.
			Cloner cloner=new Cloner(); // SHOULD HAVE BEEN INITIALIZED ONCE
			final Predictor predictorFinal=cloner.deepClone(predictor); // I like the idea that it is immutable
			
			// Calculate the predictor
			journal = getPredictor(setting, journal, predictorFinal);	// I SHOULD BE RETURNING THE JOURNAL
	
			return;
		}
				
		// Reduction step: get rid of one parameter
		String parameterName = parameterMap.firstKey();	// Pick a parameter
		TreeMap<String, String> parameterMapReduced = new TreeMap<String, String>(parameterMap); // Make a copy...
		parameterMapReduced.remove(parameterName);		// ...without the parameter
				
		for (String parameterValue : parameterMap.get(parameterName).split(",")) {
			predictor.parameterList.put(parameterName, parameterValue);			// Bind the parameter to the value
			
			loopParameters(setting, journal, predictor, parameterMapReduced); 	// Tail recursion
		}
		
		return;
	}
	
	// Subroutine 3: Recursively generate each possible combination of the columns.
	private static void loopColumns(Setting setting, Journal journal, Predictor predictor, Pattern pattern, SortedSet<String> columnSet, Table table) {
		
		// Termination condition: Empty columnList
		if (columnSet.isEmpty()) {
			loopParameters(setting, journal, predictor, pattern.getParameterMap());	// Just get the predictor.
			return;
		}
				
		// Reduction step: 
		String columnName = columnSet.first();	// Pick a column 
		SortedSet<String> reducedSet = new TreeSet<String>(columnSet);	// Make a copy of the columnList...
		reducedSet.remove(columnName);	//...without the column
		
		// Pick the right set of columns to iterate over
		// NO GUARANTIES ABOUT COLUMN1 != COLUMN2 IF THEY ARE OF THE SAME TYPE!
		SortedSet<String> columnValueSet = table.anyColumn;	// By default all columns are considered
		if (columnName.toUpperCase().matches("@NUMERICALCOLUMN\\d*")) columnValueSet = table.numericalColumn;	// Case insensitive
		if (columnName.toUpperCase().matches("@NOMINALCOLUMN\\d*")) columnValueSet = table.nominalColumn;	// Could use switch-case
		if (columnName.toUpperCase().matches("@DATECOLUMN\\d*")) columnValueSet = table.dateColumn; // Suffix with any number of digits
		if (columnName.toUpperCase().matches("@DATACOLUMN\\d*")) columnValueSet = table.dataColumn;
		if (columnName.toUpperCase().matches("@IDCOLUMN\\d*")) columnValueSet = table.idColumn;
		
		// Bind a column variable to the actual column name.  
		for (String columnValue : columnValueSet) {
			predictor.columnMap.put(columnName, columnValue);
			
			loopColumns(setting, journal, predictor, pattern, reducedSet, table); // Continue with the reduced list.
		}
		
		return;
	}

	// Subroutine 2: Loop over the propagated tables
	private static void loopTables(Setting setting, Journal journal, Predictor predictor, Pattern pattern, SortedMap<String, Table> tableMetadata) {
		
		boolean patternIsUnique = pattern.getCardinality().equals("1");	// Pattern cardinality is treated as binary
		
		// For each propagated table. There can be only one input table, hence it is not necessary to perform recursion.
		for (Table workingTable : tableMetadata.values()) {
			// Skip tables with wrong cardinality
			if (patternIsUnique != workingTable.isUnique) {
				continue;
			}

			predictor.propagatedTable = workingTable.propagatedName;
			predictor.inputTableOriginal = workingTable.originalName;
			predictor.propagationDate = workingTable.propagationDate;
			predictor.propagationPath = workingTable.propagationPath;

			loopColumns(setting, journal, predictor, pattern, pattern.getColumnSet(), workingTable); 

		}
		
		return;
	}
	
	// Subroutine 1: Loop over the patterns
	private static void loopPatterns(Setting setting, Journal journal, SortedMap<String, Table> tableMetadata) {

		File dir = new File("src/pattern");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File path : directoryListing) {			
				Pattern pattern = XML.xml2pattern(path.toString());		// Read a pattern
				Predictor predictor = new Predictor(pattern);			// Build a predictor from the pattern
				loopTables(setting, journal, predictor, pattern, tableMetadata);	// Set other parameters
			}
		} else {
			// Handle the case where directory is not really a directory.
			// Checking dir.isDirectory() would not be sufficient
			// to avoid race conditions with another process that deletes
			// directories.
		}			 
	}
	
}


