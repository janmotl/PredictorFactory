package featureExtraction;

import java.io.File;
import java.time.LocalDateTime;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;

import propagation.MetaOutput.OutputTable;
import run.Journal;
import run.Setting;

import com.rits.cloning.Cloner;

import connection.Network;
import connection.SQL;

public class Aggregation {

	// Subroutine 5: Create predictor with index and QC.
	private static void getPredictor(Setting setting, Journal journal, Predictor predictor) {
		
		// Set predictor's id & name
		predictor.setId(journal.getNextId(setting)); 	
		predictor.outputTable = "predictor" + (predictor.getId());
		
		// Set predictor's names
		predictor.setName(predictor.getNameOnce(setting));
		predictor.setLongName(predictor.getLongNameOnce());
		
		// Convert pattern to SQL
		predictor.setSql(SQL.getPredictor(setting, predictor));
		
		// Set timestamp_build
		predictor.setTimestampBuilt(LocalDateTime.now());
		
		// Execute the SQL
		predictor.setOk(Network.executeUpdate(setting.connection, predictor.getSql()));
		
		// If we know that the creation of the predictor failed, save time and just log it.
		if (predictor.isOk()) {
			// Add index
			String sql = SQL.getIndex(setting, predictor.outputTable);
			Network.executeUpdate(setting.connection, sql);
			
			// Add row count
			predictor.setRowCount(SQL.getRowCount(setting, predictor.outputTable));
			
			// Add null count
			predictor.setNullCount(predictor.getRowCount() - SQL.getNotNullCount(setting, predictor.outputTable, predictor.getName()));
			
			// Add univariate relevance estimate
			// Should be conditional on QC		
			SortedMap<String, Double> relevanceList = new TreeMap<String, Double>();
			if (setting.task.equalsIgnoreCase("classification")) {
				relevanceList.put(setting.baseTarget, SQL.getChi2(setting, predictor.outputTable, predictor.getName()));
			} else {
				relevanceList.put(setting.baseTarget, SQL.getR2(setting, predictor.outputTable, predictor.getName()));
			}
			predictor.setRelevance(relevanceList);
		}
				
		// Log the event into the journal
		journal.addPredictor(setting, predictor);
		
	}

	

	
	
	// Subroutine 4: Recursively generate each possible combination of the columns.
	private static void loopColumns(Setting setting, Journal journal, Predictor predictor,  OutputTable table, SortedSet<String> columnSet) {
		
		// Termination condition: Empty columnList
		if (columnSet.isEmpty()) {
			
			// Get an immutable copy of the predictor as we don't want to modify the template.
			// And we want to have a copy of the predictor in the journal.
			Cloner cloner=new Cloner(); // SHOULD HAVE BEEN INITIALIZED ONCE PER PF INSTANCE
			
			// Populate @value parameter if necessary.
			// Unfortunately, the order in which things have to be evaluated is: set column -> set value.
			// And currently the column is defined in the pattern (but this could be done reversely!).
			// NOTE: The value is populated only for one column ().
			if (predictor.getSql().contains("@value")) {
				List<String> valueList = SQL.getUniqueRecords(setting, predictor.propagatedTable, predictor.columnMap.get(predictor.columnMap.firstKey()));
				valueList = valueList.subList(0, Math.min(5, valueList.size())); 	// Limit the count to 5
				
				for (String value : valueList) {
					Predictor predictorVersion=cloner.deepClone(predictor);
					String sql = predictorVersion.getSql().replace("@value", value);
					predictorVersion.setSql(sql);
					predictorVersion.parameterList.put("@value", value);
					getPredictor(setting, journal, predictorVersion);
				}
				
				return;
			}
			
			Predictor predictorFinal=cloner.deepClone(predictor);
			getPredictor(setting, journal, predictorFinal);
			return;
		}
				
		// Reduction step: 
		String columnName = columnSet.first();	// Pick a column 
		SortedSet<String> reducedSet = new TreeSet<String>(columnSet);	// Make a copy of the columnList...
		reducedSet.remove(columnName);	//...without the column
		
		// Pick the right set of columns to iterate over
		// NO GUARANTIES ABOUT COLUMN1 != COLUMN2 IF THEY ARE OF THE SAME TYPE!
		// NEITHER THAT {COLUMN1, COLUMN2} WILL BE REPEATED AS {COLUMN2, COLUMN1}!
		SortedSet<String> columnValueSet = null;	// By default all columns should be considered. BUT I WANT TO MAKE SURE PATTERNS ARE OK.
		if (columnName.toUpperCase().matches("@NUMERICALCOLUMN\\d*")) columnValueSet = table.numericalColumn;	// Case insensitive
		if (columnName.toUpperCase().matches("@NOMINALCOLUMN\\d*")) columnValueSet = table.nominalColumn;	// Could use switch-case
		if (columnName.toUpperCase().matches("@TIMECOLUMN\\d*")) columnValueSet = table.timeColumn; // Suffix with any number of digits
		if (columnName.toUpperCase().matches("@IDCOLUMN\\d*")) columnValueSet = table.idColumn;
		
		// Bind a column variable to the actual column name.  
		for (String columnValue : columnValueSet) {
			predictor.columnMap.put(columnName, columnValue);
			
			loopColumns(setting, journal, predictor, table, reducedSet); // Continue with the reduced list.
		}
		
	}
	
	// Subroutine 3: Recursively generate each possible combination of the parameters.
	// MAKE SETTING & JOURNAL GLOBAL AS THERE IS JUST ONE INSTANCE OF THEM -> THE LAUNCHER SHOULD MAKE AN OBJECT
	private static void loopParameters(Setting setting, Journal journal, Predictor predictor, Pattern pattern, OutputTable workingTable, SortedMap<String, String> parameterMap) {
		
		// Termination condition: empty parameterMap - do not set any parameter, just get the predictor
		if (parameterMap.isEmpty()) {
			
			// Apply all the changes to the SQL (better to do it once than many time).
			String sql = predictor.getPatternCode();
		    for (String parameterName : predictor.getParameterList().keySet()) {
		    	// Only whole words! Otherwise "@column" would also match "@columnName".
		    	String oldString = parameterName.substring(1); // remove the at-sign as it is not considered as a part of word in regex
		    	String newString = predictor.getParameterList().get(parameterName);			    	
		    	sql = sql.replaceAll("@\\b" + oldString + "\\b", newString);
			}
		    predictor.setSql(sql); 
		    
		    // Get columnSet.
		    // Return each occurrence of "@?*column*" in SQL. The search is case sensitive (to force nicely formated patterns).
			String regexPattern = "@\\w+Column\\w*";
			SortedSet<String> columnSet = new TreeSet<String>();
			Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(sql);
			while (m.find()) {
				columnSet.add(m.group());
			}
						
			// Clean column map (necessary because of deep cloning)
			predictor.columnMap = new TreeMap<String, String>();
		    				
			// Loop over columns
			loopColumns(setting, journal, predictor, workingTable, columnSet); 	
			return;
		}
				
		// Reduction step: get rid of one parameter
		String parameterName = parameterMap.firstKey();	// Pick a parameter
		TreeMap<String, String> parameterMapReduced = new TreeMap<String, String>(parameterMap); // Make a copy...
		parameterMapReduced.remove(parameterName);		// ...without the parameter
				
		for (String parameterValue : parameterMap.get(parameterName).split(",")) {
			predictor.parameterList.put(parameterName, parameterValue);			// Bind the parameter to the value
			
			loopParameters(setting, journal, predictor, pattern, workingTable, parameterMapReduced); 	// Tail recursion
		}
		
	}

	// Subroutine 2: Loop over the propagated tables
	private static void loopTables(Setting setting, Journal journal, Predictor predictor, Pattern pattern, SortedMap<String, OutputTable> tableMetadata) {
		
		// For each propagated table. There is always just one input table, hence it is not necessary to perform recursion.
		for (OutputTable workingTable : tableMetadata.values()) {

			// Skip tables with wrong cardinality
			if ((pattern.cardinality.equals("1") && !workingTable.isUnique) || (pattern.cardinality.equals("n") && workingTable.isUnique)) {
				continue;	
			}
		
			predictor.propagatedTable = workingTable.propagatedName;
			predictor.originalTable = workingTable.originalName;
			predictor.propagationDate = workingTable.propagationDate;
			predictor.propagationPath = workingTable.propagationPath;

			// Loop over a list of parameters, which was already modified to be compatible with the destination database.
			// Note: Loop first over parameters, then over columns, as assignment of columns can happen in parameters
			// like in directField pattern.
			loopParameters(setting, journal, predictor, pattern, workingTable, pattern.dialectParameter);
		}
		
	}
	
	// Subroutine 1: Loop over the patterns
	public static void loopPatterns(Setting setting, Journal journal, SortedMap<String, OutputTable> tableMetadata) {

		File dir = new File("src/pattern");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File path : directoryListing) {			
				Pattern pattern = Pattern.unmarshall(path.toString());		// Read a pattern
				pattern.agnostic2dialectCode(setting);					// Initialize dialectCode. Once.
				Predictor predictor = new Predictor(pattern);			// Build a predictor from the pattern
				loopTables(setting, journal, predictor, pattern, tableMetadata);	// Set other parameters
			}
		}
	}
	
}
