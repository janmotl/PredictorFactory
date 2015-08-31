package featureExtraction;

import com.rits.cloning.Cloner;
import connection.Network;
import connection.SQL;
import featureExtraction.Pattern.OptimizeParameters;
import metaInformation.MetaOutput.OutputTable;
import org.apache.log4j.Logger;
import run.Journal;
import run.Setting;
import utility.PatternMap;

import java.time.LocalDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;

public class Aggregation {
	// Logging
	private static final Logger logger = Logger.getLogger(Aggregation.class.getName());

	// Deep cloning
	private static Cloner cloner = new Cloner(); 
	
	
	
	// Do it
	public static void aggregate(Setting setting, Journal journal, SortedMap<String, OutputTable> tableMetadata ) {
		
		// Initialization
		int groupId = 1;
		
		// 1) Get predictors
		List<Predictor> predictorList = loopPatterns(setting);
		
		// 2) Set @targetValue
			// First, get the cached list of unique values in the target column
			List<String> uniqueList = new ArrayList<String>();
			for (OutputTable table : tableMetadata.values()) {
				if (setting.targetTable.equals(table.originalName)) {
					uniqueList = table.uniqueList.get(setting.targetColumn);
					break; // No need to continue
				}
			}

			// Second, Loop over all the predictors
			List<Predictor> predictorList2 = new ArrayList<Predictor>();
			for (Predictor predictor : predictorList) {
				predictorList2.addAll(addTargetValue(setting, predictor, uniqueList));
			}

		
		// 3) Loop over parameters
		List<Predictor> predictorList3 = new ArrayList<Predictor>(); 
		for (Predictor predictor : predictorList2) {
			predictorList3.addAll(loopParameters(predictor));
		}
		
		// 4) Loop over tables
		List<Predictor> predictorList4 = new ArrayList<Predictor>(); 
		for (Predictor predictor : predictorList3) {
			predictorList4.addAll(loopTables(predictor, tableMetadata));
		}
		
		// 5) Loop over columns
		List<Predictor> predictorList5 = new ArrayList<Predictor>(); 
		for (Predictor predictor : predictorList4) {
			predictorList5.addAll(loopColumns(predictor, tableMetadata));
		}
		
		// 6) Loop over @value
		List<Predictor> predictorList6 = new ArrayList<Predictor>(); 
		for (Predictor predictor : predictorList5) {
			predictorList6.addAll(addValue(setting, predictor, tableMetadata));
		}
		
		// 7) Optimize parameters
		List<Predictor> predictorList7 = new ArrayList<Predictor>(); 
		for (Predictor predictor : predictorList6) {
			predictorList7.addAll(optimizeAll(predictor, groupId));
			groupId++; // GroupId is unique per optimization group
		}
		

		// 8) Execute the SQL & log the result
		int maxRowLimit = SQL.getRowCount(setting, setting.outputSchema, setting.baseTable); // For QC purposes
		
		for (Predictor predictor : predictorList7) {
			materializePredictor(setting, journal, predictor, maxRowLimit); 
			journal.addPredictor(setting, predictor);
		}
		
	}
	
	
	// Subroutine 1: Get list of all the patterns in the pattern directory. Return list of predictors.
	protected static List<Predictor> loopPatterns(Setting setting) {

		// Initialize
		List<Predictor> outputList = new ArrayList<Predictor>();
		SortedMap<String, Pattern> patternMap = PatternMap.getPatternMap();

		// If whitelist is not empty, perform intersect
		if (!setting.whiteListPattern.isEmpty()) {
			patternMap.keySet().retainAll(Arrays.asList(setting.whiteListPattern.split(",")));
		}
		
		// Skip blacklisted patterns
		String[] blackList = setting.blackListPattern.split(",");
		patternMap.keySet().removeAll(Arrays.asList(blackList));
		
		// Get the dialect code
		for (Pattern pattern : patternMap.values()) {
			pattern.agnostic2dialectCode(setting);					// Initialize dialectCode. Once.
			outputList.add(new Predictor(pattern));					// Build a predictor from the pattern
		}
		
		return outputList;
	}
	
	
	// Subroutine 2: Populate @targetValue (based on unique values in the target column in the target window) and set SQL.
	// If we are performing regression, remove patterns that are using target value, as these patterns are not applicable.
	// CURRENTLY IMPLEMENTED ONLY FOR THE FIRST VALUE -> IT RETURNS JUST A SINGLE PREDICTOR
	protected static List<Predictor> addTargetValue(Setting setting, Predictor predictor, List<String> uniqueList){

		boolean isClassification = "classification".equals(setting.task);
		boolean containsTargetValue = predictor.getPatternCode().contains("@targetValue");

		// If the pattern uses target value...
		if (containsTargetValue) {
			if (isClassification) {
				predictor.setParameter("@targetValue", uniqueList.get(0));
			} else {
				return new ArrayList<Predictor>();
			}
		}

		// Add that one predictor into a list
		List<Predictor> result = new ArrayList<>();
		result.add(predictor);

		return result;
	}
	
	
	// Subroutine 3: Recursively generate each possible combination of the parameters.
	protected static List<Predictor> loopParameters(Predictor predictor) {
		
		// Initialize the output
		List<Predictor> predictorList = new ArrayList<Predictor>();
		predictorList.add(predictor);
		
		// Loop over each parameter
		for (Entry<String, String> parameter : predictor.getPatternParameterList().entrySet()) {
			predictorList = expandParameter(predictorList, parameter.getKey(), parameter.getValue().split(","));
		}
		
		// Apply the parameters to SQL
		// This is necessary because some parameters can define columns.
		for (Predictor pred : predictorList) {
			 pred = addSQL(pred, pred.getPatternCode());
		}
		
		return predictorList;
	}
	
	private static List<Predictor> expandParameter(List<Predictor> predictorList, String parameterName, String[] parameterValueList) {
		
		List<Predictor> outputList = new ArrayList<Predictor>();
		
		for (Predictor predictor : predictorList) {
			for (String parameterValue : parameterValueList) {
				
				Predictor cloned = cloner.deepClone(predictor);
				cloned.setParameter(parameterName, parameterValue);
				outputList.add(cloned);
				
			}
		}
		
		return outputList;
	}
	
	
	// Subroutine 4: Loop over the propagated tables
	protected static List<Predictor> loopTables(Predictor predictor, SortedMap<String, OutputTable> tableMetadata) {
		
		// Initialize the output
		List<Predictor> outputList = new ArrayList<Predictor>();
		
		// For each propagated table.
		for (OutputTable workingTable : tableMetadata.values()) {

			// Skip tables with wrong cardinality
			String cardinality = predictor.getPatternCardinality();
			if (("1".equals(cardinality) && !workingTable.isUnique) || ("n".equals(cardinality) && workingTable.isUnique)) {
				continue;	
			}
			
			// Clone
			Predictor cloned = cloner.deepClone(predictor);
		
			// Store the necessary data into the clone
			cloned.propagatedTable = workingTable.propagatedName;
			cloned.originalTable = workingTable.originalName;
			cloned.propagationDate = workingTable.constrainDate;
			cloned.propagationPath = workingTable.propagationPath;
			
			// Store the clone
			outputList.add(cloned);
		}
		
		return outputList;
	}
	
	
	// Subroutine 5: Recursively generate each possible combination of the columns.
	// NO GUARANTIES ABOUT COLUMN1 != COLUMN2 IF THEY ARE OF THE SAME TYPE!
	// NEITHER THAT {COLUMN1, COLUMN2} WILL BE REPEATED AS {COLUMN2, COLUMN1}!
	protected static List<Predictor> loopColumns(Predictor predictor, SortedMap<String, OutputTable> tableMetadata) {
		
		// Initialize the output
		List<Predictor> predictorList = new ArrayList<Predictor>();
		predictorList.add(predictor);
		
	    // Find each occurrence of "@?*column*" in SQL. The search is case sensitive (to force nicely formated patterns).
		String regexPattern = "@\\w+Column\\w*";
		Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(predictor.getSql());
		while (m.find()) {
			predictor.columnMap.put(m.group(), null);
		}
		
		// Loop over each column in the predictor
		for (Entry<String, String> parameter : predictor.columnMap.entrySet()) {
			predictorList = expandColumn(predictorList, parameter.getKey(), tableMetadata.get(predictor.propagatedTable));
		}
		
		return predictorList;		
	}
	
	private static List<Predictor> expandColumn(List<Predictor> predictorList, String columnKey, OutputTable table) {
		
		List<Predictor> outputList = new ArrayList<Predictor>();
		
		for (Predictor predictor : predictorList) {
			// Select columns from the table with the correct data type. 	
			SortedSet<String> columnValueSet = new TreeSet<String>();
			
			// Matches: Case insensitive plus suffix with any number of digits
			if (columnKey.matches("(?i)@NUMERICALCOLUMN\\d*")) columnValueSet = table.numericalColumn;	
			else if (columnKey.matches("(?i)@NOMINALCOLUMN\\d*")) columnValueSet = table.nominalColumn;	
			else if (columnKey.matches("(?i)@TIMECOLUMN\\d*")) columnValueSet = table.timeColumn; 
			else if (columnKey.matches("(?i)@IDCOLUMN\\d*")) columnValueSet = table.idColumn;
			else logger.warn("The term: " + columnKey +  " in pattern: " + predictor.getPatternName() + " is not recognized as a valid column identifier. Expected terms are: {numericalColumn, nominalColumn, timeColumn, idColumn}.");
			
			// Bind the columnKey to the actual columnValue.  		
			for (String columnValue : columnValueSet) {
				Predictor cloned = cloner.deepClone(predictor);
				cloned.columnMap.put(columnKey, columnValue);
				outputList.add(cloned);
			}
		}
		
		return outputList;
	}
	
	
	// Subroutine 6: Populate @value parameter.
	// NOTE: The value is populated only for the first column in the columnMap. 
	protected static List<Predictor> addValue(Setting setting, Predictor predictor, SortedMap<String, OutputTable> tableMetadata) {
		
		// Initialize the output
		List<Predictor> predictorList = new ArrayList<Predictor>();

		// If the pattern code contains @value
		if (predictor.getPatternCode().contains("@value")) {
			for (Entry<String, String> column : predictor.columnMap.entrySet()) {
				// Pick a nominal column 
				if (column.getKey().toUpperCase().matches("@NOMINALCOLUMN\\d*")) {
					
					// Get list of unique values
					List<String> valueList = tableMetadata.get(predictor.propagatedTable).uniqueList.get(column.getValue());
					
					// Limit the count to a manageable value
					valueList = valueList.subList(0, Math.min(setting.valueCount, valueList.size())); 

					for (String value : valueList) {
						Predictor cloned = cloner.deepClone(predictor);
						cloned.setParameter("@value", value);
						predictorList.add(cloned);
					}
					
					break; // Just the first nominalColumn. Remember?
				}
			}
			
			// Apply the new parameters to SQL
			for (Predictor pred : predictorList) {
				 pred = addSQL(pred, pred.getSql());
			}
		} else {
			predictorList.add(predictor);
		}

		return predictorList;
	}

	
	// Subroutine 7: Optimize the parameter.
	private static List<Predictor> optimizeAll(Predictor predictor, int groupId) {
		// Initialization
		List<Predictor> outputList = new ArrayList<Predictor>();
		outputList.add(predictor);
		
		// Loop over all parameters to optimize
		for (OptimizeParameters parameter : predictor.getPatternOptimizeList()) {
			outputList = optimize(outputList, parameter);
		}
		
		// Apply the parameters to SQL (Just like in Parameter section)
		for (Predictor pred : outputList) {
			 pred = addSQL(pred, pred.getPatternCode());
		}
		
		// Set predictor's group id
		for (Predictor pred : outputList) {
			 pred.setGroupId(groupId);
		}
		
		return outputList;
	}
	
	private static List<Predictor> optimize(List<Predictor> predictorList, OptimizeParameters parameter) {
		// Initialization
		List<Predictor> outputList = new ArrayList<Predictor>();
		
		for (Predictor predictor : predictorList) {
			// CURRENTLY JUST SWEEP OVER THE VALUES. USE A REAL OPTIMALISATION TOOLBOX.
			for (int i = 0; i < parameter.iterationLimit; i++) {
				double value = parameter.min + i * (parameter.max-parameter.min)/(parameter.iterationLimit-1);
				Predictor clonedPredictor = cloner.deepClone(predictor);
				clonedPredictor.setParameter(parameter.key, String.valueOf(value));
				outputList.add(clonedPredictor);
			}
		}
			
		return outputList;
	}
	

	// Subroutine 8: Create predictor with index and QC.
	private static void materializePredictor(Setting setting, Journal journal, Predictor predictor, int maxRowLimit) {
				
		// Set predictor's id & table name
		predictor.setId(journal.getNextId(setting)); 	
		predictor.outputTable = setting.predictorPrefix + (predictor.getId());
		
		// Set predictor's names
		predictor.setName(predictor.getNameOnce(setting));
		predictor.setLongName(predictor.getLongNameOnce());
		
		// Set default relevance value for the target. 
		predictor.setRelevance(setting.baseTarget, 0.0);
		
		// Convert pattern to SQL
		predictor.setSql(SQL.getPredictor(setting, predictor));
		
		// Set timestamp_build
		predictor.setTimestampBuilt(LocalDateTime.now());
		
		// Execute the SQL
		predictor.setOk(Network.executeUpdate(setting.connection, predictor.getSql()));
		
		// If the execution failed, stop.
		if (!predictor.isOk()) return;
		
		// Add Primary Key constrain.
		// This is not because of speeding things up (indeed it has a negative impact on total runtime because only
		// a small proportion of the predictors gets into MainSample) but because it validates uniqueness of the tuples.
		// NOTE: AZURE REQUIRES TO FIRST SET NOT NULL -> SKIPPING IT FOR MSSQL.
		if (!"Microsoft SQL Server".equals(setting.databaseVendor) && !SQL.setPrimaryKey(setting, predictor.outputTable)) {
			logger.warn("Primary key constrain failed");
			return;
		}
		
		// Add row count
		predictor.setRowCount(SQL.getRowCount(setting, setting.outputSchema, predictor.outputTable));
		if (predictor.getRowCount()==0) return;
		if (predictor.getRowCount()>maxRowLimit) {
			logger.warn("Predictor " + predictor.getName() + " has " + predictor.getRowCount() + " rows. But base table has only " + maxRowLimit + " rows.");
			return;
		}
		
		// Add null count
		predictor.setNullCount(predictor.getRowCount() - SQL.getNotNullCount(setting, setting.outputSchema, predictor.outputTable, predictor.getName()));
		if (predictor.getNullCount()==predictor.getRowCount()) return;
			
		// Add univariate relevance estimate
		if ("classification".equalsIgnoreCase(setting.task)) {
			predictor.setRelevance(setting.baseTarget, SQL.getChi2(setting, predictor.outputTable, predictor.getName()));
		} else {
			predictor.setRelevance(setting.baseTarget, SQL.getR2(setting, predictor.outputTable, predictor.getName()));
		}
	}

	
	// Sub-Subroutine: Reflect settings in columnMap & parameterMap into SQL
	protected static Predictor addSQL(Predictor predictor, String sql) {
		
	    for (String parameterName : predictor.getParameterMap().keySet()) {
	    	// Only whole words! Otherwise "@column" would also match "@columnName".
	    	String oldString = parameterName.substring(1); // remove the at-sign as it is not considered as a part of word in regex
	    	String newString = predictor.getParameterMap().get(parameterName);			    	
	    	sql = sql.replaceAll("@\\b" + oldString + "\\b", newString);
		}
	    predictor.setSql(sql); 
	    
	    return predictor;
	}
}
	