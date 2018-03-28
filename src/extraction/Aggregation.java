package extraction;

import connection.Network;
import extraction.Pattern.OptimizeParameters;
import meta.Column;
import meta.OutputTable;
import meta.StatisticalType;
import meta.Table;
import org.apache.log4j.Logger;
import run.Setting;
import utility.BlackWhiteList;
import utility.Meta;
import utility.PatternMap;
import utility.TextParser;

import javax.xml.bind.JAXBException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static java.util.regex.Pattern.compile;

public class Aggregation {
	// Logging
	private static final Logger logger = Logger.getLogger(Aggregation.class.getName());

	// Each predictor has a unique id
	private static int id;

	// Compiled regex patterns. Case sensitive to enforce nicely formatted patterns.
	// We use 'Column' suffix to distinguish between: '@timestamp' and '@timestampWithTimezone' without the need to
	// think about the order of pattern matching.
	private static final java.util.regex.Pattern regexPatternColumn = compile("@\\w+Column\\d*");
	private static final java.util.regex.Pattern regexPatternAny = compile("@anyColumn\\d*");
	private static final java.util.regex.Pattern regexPatternCharacter = compile("@characterColumn\\d*");
	private static final java.util.regex.Pattern regexPatternNumerical = compile("@numericalColumn\\d*");
	private static final java.util.regex.Pattern regexPatternNominal = compile("@nominalColumn\\d*");
	private static final java.util.regex.Pattern regexPatternTemporal = compile("@temporalColumn\\d*");
	private static final java.util.regex.Pattern regexPatternLongnvarchar = compile("@longnvarcharColumn\\d*");
	private static final java.util.regex.Pattern regexPatternNchar = compile("@ncharColumn\\d*");
	private static final java.util.regex.Pattern regexPatternNvarchar = compile("@nvarcharColumn\\d*");
	private static final java.util.regex.Pattern regexPatternTinyint = compile("@tinyintColumn\\d*");
	private static final java.util.regex.Pattern regexPatternBigint = compile("@bigintColumn\\d*");
	private static final java.util.regex.Pattern regexPatternLongvarchar = compile("@longvarcharColumn\\d*");
	private static final java.util.regex.Pattern regexPatternChar = compile("@charColumn\\d*");
	private static final java.util.regex.Pattern regexPatternNumeric = compile("@numericColumn\\d*");
	private static final java.util.regex.Pattern regexPatternDecimal = compile("@decimalColumn\\d*");
	private static final java.util.regex.Pattern regexPatternInteger = compile("@integerColumn\\d*");
	private static final java.util.regex.Pattern regexPatternSmallint = compile("@smallintColumn\\d*");
	private static final java.util.regex.Pattern regexPatternFloat = compile("@floatColumn\\d*");
	private static final java.util.regex.Pattern regexPatternReal = compile("@realColumn\\d*");
	private static final java.util.regex.Pattern regexPatternDouble = compile("@doubleColumn\\d*");
	private static final java.util.regex.Pattern regexPatternVarchar = compile("@varcharColumn\\d*");
	private static final java.util.regex.Pattern regexPatternBoolean = compile("@booleanColumn\\d*");
	private static final java.util.regex.Pattern regexPatternDate = compile("@dateColumn\\d*");
	private static final java.util.regex.Pattern regexPatternTime = compile("@timeColumn\\d*");
	private static final java.util.regex.Pattern regexPatternTimestamp = compile("@timestampColumn\\d*");
	private static final java.util.regex.Pattern regexPatternSqlxml = compile("@sqlxmlColumn\\d*");
	private static final java.util.regex.Pattern regexPatternTimeWithTimezone = compile("@timeWithTimezoneColumn\\d*");
	private static final java.util.regex.Pattern regexPatternTimestampWithTimezone = compile("@timestampWithTimezoneColumn\\d*");
	private static final java.util.regex.Pattern regexPatternEnum = compile("@enumColumn\\d*");
	private static final java.util.regex.Pattern regexPatternInterval = compile("@intervalColumn\\d*");
	private static final java.util.regex.Pattern regexPatternSet = compile("@setColumn\\d*");
	private static final java.util.regex.Pattern regexPatternYear = compile("@yearColumn\\d*");

	public static Journal run(Setting setting, List<OutputTable> tableMetadata) {
		id = setting.predictorStart;

		// Try to reuse journal.xml if possible. Fallback to processing from scratch.
		try {
			List<Predictor> topPredictors = Journal.unmarshall(setting).getAllTopPredictors();
			Journal journal = new Journal(setting, topPredictors.size());
			return fromXML(setting, journal, topPredictors);
		} catch (InstantiationException | JAXBException e) {
			return fromScratch(setting, tableMetadata);
		}
	}

	private static Journal fromScratch(Setting setting, List<OutputTable> tableMetadata ) {

		// Initialization
		Journal journal;    // Log of all predictors
		int groupId = 1;    // To group different refinements together


		// 1) Get predictors
		List<Predictor> predictorList = loopPatterns(setting);
		// Log patterns
		setting.dialect.getJournalPattern(setting);
		setting.dialect.addToJournalPattern(setting, predictorList);

		// 2) Set @targetValue
		List<Predictor> predictorList2 = new ArrayList<>();
		for (Predictor predictor : predictorList) {
			predictorList2.addAll(addTargetValue(setting, predictor));
		}

		// 3) Loop over parameters
		List<Predictor> predictorList3 = new ArrayList<>();
		for (Predictor predictor : predictorList2) {
			predictorList3.addAll(loopParameters(predictor));
		}

		// 4) Loop over tables
		List<Predictor> predictorList4 = new ArrayList<>();
		for (Predictor predictor : predictorList3) {
			predictorList4.addAll(loopTables(predictor, tableMetadata));
		}

		// 5) Loop over columns
		List<Predictor> predictorList5 = new ArrayList<>();
		for (Predictor predictor : predictorList4) {
			predictorList5.addAll(loopColumns(setting, predictor));
		}

		// 6) Loop over @value
		List<Predictor> predictorList6 = new ArrayList<>();
		for (Predictor predictor : predictorList5) {
			predictorList6.addAll(addValue(setting, predictor));
		}

		// 7) Optimize parameters
		List<Predictor> predictorList7 = new ArrayList<>();
		for (Predictor predictor : predictorList6) {
			predictorList7.addAll(optimizeAll(predictor, groupId));
			groupId++; // GroupId is unique per optimization group
		}


		// 8) Execute the SQL & log the result
		int maxRowLimit = setting.dialect.getRowCount(setting, setting.outputSchema, setting.baseSampled);
		journal = new Journal(setting, predictorList7.size());

		for (Predictor predictor : predictorList7) {
			materializePredictor(setting, predictor, maxRowLimit);
			journal.addPredictor(setting, predictor);
		}

		return journal;
	}

	private static Journal fromXML(Setting setting, Journal journal, List<Predictor> topPredictors) {
		// 8) Execute the SQL & log the result
		int maxRowLimit = setting.dialect.getRowCount(setting, setting.outputSchema, setting.baseSampled);

		for (Predictor predictor : topPredictors) {
			predictor.getPattern().initialize(setting); // NOT THE NICEST...
			materializePredictor2(setting, predictor, maxRowLimit);
			journal.addPredictor(setting, predictor);
		}

		return journal;
	}


	// Subroutine 1: Get list of all the patterns in the pattern directory. Return list of predictors.
	protected static List<Predictor> loopPatterns(Setting setting) {

		// Initialize
		List<Predictor> outputList = new ArrayList<>();
		SortedMap<String, Pattern> patternMap = PatternMap.getPatternMap();

		// Apply black/white lists
		List blackList = TextParser.string2list(setting.blackListPattern);
		List whiteList = TextParser.string2list(setting.whiteListPattern);
		patternMap = BlackWhiteList.filter(patternMap, blackList, whiteList);

		// Get the dialect code
		for (Pattern pattern : patternMap.values()) {
			pattern.initialize(setting);                // Initialize dialectCode. Once.
			outputList.add(new Predictor(pattern));     // Build a predictor from the pattern
		}

		// Skip predictors requiring baseDate, if targetDate is not available
		outputList = filterRequiresBaseDate(setting, outputList);

		// If we use a pattern that is using @targetValue and @timeColumn,
		// warn the user that we are using lagged values of the target.
		if (outputList.stream().anyMatch(it -> it.getPatternCode().contains("@targetValue"))) {
			logger.info("Predictors that use the lagged target are known to have inflated estimates of the weights if OLS (Ordinary Least Square regression) is used. The remedy is to use MLE (Maximum Likelihood Estimate method) to estimate the parameters (see The Overlapping Data Problem by Ardian, or A Note on the Validity of Cross-Validation for Evaluating Time Series Prediction by Bergmeir, or Why Lagged Dependent Variables Can Supress the Explanatory Power of Other Independent Variables by Achen). Methods that are based on gradient descend (e.g. logistic regression) should by definition not be heavily biased (the regularization factor, however, is affected), although convergence may take longer.");
		}

		return outputList;
	}

	private static List<Predictor> filterRequiresBaseDate(Setting setting, List<Predictor> predictorList) {
		if (setting.targetDate == null) {
			return predictorList.stream().filter(predictor -> !predictor.getPatternRequiresBaseDate()).collect(Collectors.toList());
		}

		return predictorList;
	}


	// Subroutine 2: Populate @targetValue (based on unique values in the target column in the target window) and set SQL.
	// If we are performing regression, remove patterns that are using target value, as these patterns are not applicable.
	// CURRENTLY IMPLEMENTED ONLY FOR THE FIRST VALUE -> IT RETURNS JUST A SINGLE PREDICTOR
	protected static List<Predictor> addTargetValue(Setting setting, Predictor predictor){
		List<Predictor> result = new ArrayList<>();
		boolean isClassification = "classification".equals(setting.task);
		boolean containsTargetValue = predictor.getPatternCode().contains("@targetValue");

		// If the pattern uses target value and we are performing classification...
		if (containsTargetValue && isClassification) {
			for (int i = 0; i < setting.baseTargetList.size(); i++) {
				String baseTarget = setting.baseTargetList.get(i);
				String targetColumn = setting.targetColumnList.get(i);
				String targetValue = setting.targetUniqueValueMap.get(targetColumn).keySet().iterator().next(); // Just pick the most common value
				Predictor cloned = new Predictor(predictor);
				cloned.setBaseTarget(baseTarget);
				cloned.setTargetColumn(targetColumn);
				cloned.setParameter("@targetValue", targetValue);

				// Set targetValuePrior
				if (predictor.getPatternCode().contains("@targetValuePrior")) {
					String targetValuePrior = Double.toString(getPrior(setting.targetUniqueValueMap.get(targetColumn), targetValue));
					cloned.setParameter("@targetValuePrior", targetValuePrior);
				}

				result.add(cloned);
			}
		} else {
			result.add(predictor);
		}

		return result;
	}

	// Should have been calculated just once and stored in setting or somewhere
	private static double getPrior(LinkedHashMap<String, Integer> uniqueValueMap, String value) {
		int counter = 0;

		for (Integer count : uniqueValueMap.values()) {
			counter = counter + count;
		}

		return ( ((double) uniqueValueMap.get(value)) / ((double) counter) );
	}


	// Subroutine 3: Recursively generate each possible combination of the parameters.
	protected static List<Predictor> loopParameters(Predictor predictor) {

		// Initialize the output
		List<Predictor> predictorList = new ArrayList<>();
		predictorList.add(predictor);

		// Loop over each parameter
		for (Entry<String, String> parameter : predictor.getPatternParameterMap().entrySet()) {
			predictorList = expandParameter(predictorList, parameter.getKey(), parameter.getValue().split(","));
		}

		// Apply the parameters to SQL
		// This is necessary because some parameters can define columns.
		for (Predictor pred : predictorList) {
			addSQL(pred, pred.getPatternCode());
		}

		return predictorList;
	}

	private static List<Predictor> expandParameter(List<Predictor> predictorList, String parameterName, String[] parameterValueList) {

		List<Predictor> outputList = new ArrayList<>();

		for (Predictor predictor : predictorList) {
			for (String parameterValue : parameterValueList) {

				Predictor cloned = new Predictor(predictor);
				cloned.setParameter(parameterName, parameterValue);
				outputList.add(cloned);

			}
		}

		return outputList;
	}


	// Subroutine 4: Loop over the propagated tables
	protected static List<Predictor> loopTables(Predictor predictor, List<OutputTable> tableMetadata) {

		// Initialize the output
		List<Predictor> outputList = new ArrayList<>();

		// For each propagated table.
		for (OutputTable workingTable : tableMetadata) {

			// Skip tables with wrong cardinality
			String cardinality = predictor.getPatternCardinality();
			if (("1".equals(cardinality) && !workingTable.isTargetIdUnique) || ("n".equals(cardinality) && workingTable.isTargetIdUnique)) {
				continue;
			}

			// Store the table into the clone
			Predictor cloned = new Predictor(predictor);
			cloned.setTable(workingTable);
			outputList.add(cloned);
		}

		return outputList;
	}


	// Subroutine 5: Recursively generate each possible combination of the columns.
	// NO GUARANTIES THAT {COLUMN1, COLUMN2} WILL NOT BE REPEATED AS {COLUMN2, COLUMN1}!
	// Solution: introduce a mark into a pattern: isCommutative=true / isCommutative=false to optionally allow it
	// E.g. ratio is not commutative, because we may get division by zero.
	// On the other end, diff is commutative (from the point of the classifier)
	protected static List<Predictor> loopColumns(Setting setting, Predictor predictor) {

		// Initialize the output
		List<Predictor> predictorList = new ArrayList<>();
		predictorList.add(predictor);

		// Find each occurrence of "@*column*" in SQL
		Matcher m = regexPatternColumn.matcher(predictor.getSql());
		while (m.find()) {
			predictor.getColumnMap().put(m.group(), null);
		}

		// Loop over each column in the predictor
		for (Entry<String, String> parameter : predictor.getColumnMap().entrySet()) {
			predictorList = expandColumn(setting, predictorList, parameter.getKey(), predictor.getTable());
		}

		// Remove predictors, where the same column was used repeatedly
		predictorList.removeIf(it -> it.getColumnMap().values().stream().distinct().collect(Collectors.toList()).size() < it.getColumnMap().size());

		// If boolean/bit is used as numerical, cast the boolean/bit to double
		booleanToDouble(predictorList);

		return predictorList;
	}

	private static List<Predictor> expandColumn(Setting setting, List<Predictor> predictorList, String columnKey, OutputTable table) {

		List<Predictor> outputList = new ArrayList<>();

		for (Predictor predictor : predictorList) {
			// Select columns from the table with the correct data type.
			SortedSet<Column> columnValueSet = new TreeSet<>();

			// Matches: Case sensitive plus suffix with any number of digits
			if (regexPatternAny.matcher(columnKey).matches()) columnValueSet = table.getColumns();
			else if (regexPatternCharacter.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.CHARACTER);
			else if (regexPatternNumerical.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.NUMERICAL);
			else if (regexPatternNominal.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.NOMINAL);
			else if (regexPatternTemporal.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.TEMPORAL);

			else if (regexPatternLongnvarchar.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.LONGNVARCHAR);
			else if (regexPatternNchar.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.NCHAR);
			else if (regexPatternNvarchar.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.NVARCHAR);
			else if (regexPatternTinyint.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.TINYINT);
			else if (regexPatternBigint.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.BIGINT);
			else if (regexPatternLongvarchar.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.LONGVARCHAR);
			else if (regexPatternChar.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.CHAR);
			else if (regexPatternNumeric.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.NUMERIC);
			else if (regexPatternDecimal.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.DECIMAL);
			else if (regexPatternInteger.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.INTEGER);
			else if (regexPatternSmallint.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.SMALLINT);
			else if (regexPatternFloat.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.FLOAT);
			else if (regexPatternReal.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.REAL);
			else if (regexPatternDouble.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.DOUBLE);
			else if (regexPatternVarchar.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.VARCHAR);
			else if (regexPatternBoolean.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.BOOLEAN);
			else if (regexPatternDate.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.DATE);
			else if (regexPatternTime.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.TIME);
			else if (regexPatternTimestamp.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.TIMESTAMP);
			else if (regexPatternSqlxml.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.SQLXML);
			else if (regexPatternTimeWithTimezone.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.TIME_WITH_TIMEZONE);
			else if (regexPatternTimestampWithTimezone.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.TIMESTAMP_WITH_TIMEZONE);

			else if (regexPatternEnum.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.ENUM);
			else if (regexPatternInterval.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.INTERVAL);
			else if (regexPatternSet.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.SET);
			else if (regexPatternYear.matcher(columnKey).matches()) columnValueSet = table.getColumns(setting, StatisticalType.YEAR);

			else logger.warn("The term: '" + columnKey +  "' in pattern: '" + predictor.getPatternName() + "' is not recognized as a valid column identifier.");

			// Bind the columnKey to the actual columnValue.
			for (Column columnValue : columnValueSet) {
				Predictor cloned = new Predictor(predictor);
				cloned.getColumnMap().put(columnKey, columnValue.name);
				outputList.add(cloned);
			}
		}

		return outputList;
	}

	// If a boolean/bit is used as numerical, convert the boolean/bit to double
	private static void booleanToDouble(List<Predictor> predictorList) {
		for (Predictor predictor : predictorList) {
			for (Entry<String, String> tuple : predictor.getColumnMap().entrySet()) {
				String variableName = tuple.getKey();
				String attributeName = tuple.getValue();
				int columnDataType = predictor.getTable().getColumn(attributeName).dataType;
				if (variableName.startsWith("@numericalColumn") && (columnDataType == -7 || columnDataType == 16)) {
					java.util.regex.Pattern regex = compile("(\\w*\\.?" + variableName + ")");  // Just this @numericalColumn? and not other @numericalColumns
					String sql = regex.matcher(predictor.getSql()).replaceAll("CASE WHEN $1 THEN 1.0 ELSE 0.0 END");
					predictor.setSql(sql);
				}
			}
		}
	}


	// Subroutine 6: Populate @value parameter.
	// NOTE: The value is populated only for the first column in the columnMap.
	protected static List<Predictor> addValue(Setting setting, Predictor predictor) {

		// Initialize the output
		List<Predictor> predictorList = new ArrayList<>();

		// If the pattern code contains @value
		if (predictor.getPatternCode().contains("@value")) {
			for (Entry<String, String> column : predictor.getColumnMap().entrySet()) {
				// Pick a nominal column
				if (column.getKey().toUpperCase().matches("@NOMINALCOLUMN\\d*")) {

					// Get list of unique values
					Set<String> valueList = predictor.getTable().getColumn(column.getValue()).getUniqueValues(setting).keySet();

					for (String value : valueList) {
						Predictor cloned = new Predictor(predictor);
						cloned.setParameter("@value", value);
						predictorList.add(cloned);
					}

					break; // Just the first nominalColumn. Remember?
				}
			}

			// Apply the new parameters to SQL
			for (Predictor pred : predictorList) {
				addSQL(pred, pred.getSql());
			}
		} else {
			predictorList.add(predictor);
		}

		return predictorList;
	}


	// Subroutine 7: Optimize the parameter.
	private static List<Predictor> optimizeAll(Predictor predictor, int groupId) {
		// Initialization
		List<Predictor> outputList = new ArrayList<>();
		outputList.add(predictor);

		// Loop over all parameters to optimize
		for (OptimizeParameters parameter : predictor.getPatternOptimizeParameter()) {
			outputList = optimize(outputList, parameter);
		}

		// Apply the parameters to SQL (Just like in Parameter section)
		for (Predictor pred : outputList) {
			addSQL(pred, pred.getSql());
		}

		// Set predictor's group id
		for (Predictor pred : outputList) {
			pred.setGroupId(groupId);
		}

		return outputList;
	}

	private static List<Predictor> optimize(List<Predictor> predictorList, OptimizeParameters parameter) {
		// Initialization
		List<Predictor> outputList = new ArrayList<>();

		for (Predictor predictor : predictorList) {
			// CURRENTLY JUST SWEEP OVER THE VALUES. USE A REAL OPTIMISATION TOOLBOX.
			// Check:
			//  http://www.cs.ubc.ca/labs/beta/Projects/SMAC/
			//  https://github.com/deeplearning4j/Arbiter
			//  https://cs.gmu.edu/~eclab/projects/ecj/
			// IF iterationLimit is 1, we get an error.
			for (int i = 0; i < parameter.iterationLimit; i++) {
				Predictor clonedPredictor = new Predictor(predictor);

				if (parameter.integerValue) {
					int value = (int) (parameter.min + i * (parameter.max - parameter.min) / (parameter.iterationLimit - 1));
					clonedPredictor.setParameter(parameter.key, String.valueOf(value));

				} else {
					double value = parameter.min + i * (parameter.max - parameter.min) / (parameter.iterationLimit - 1);
					clonedPredictor.setParameter(parameter.key, String.valueOf(value));
				}

				outputList.add(clonedPredictor);
			}
		}

		return outputList;
	}


	// Subroutine 8: Create predictor with index and QC.
	private static void materializePredictor(Setting setting, Predictor predictor, int maxRowLimit) {

		// Set predictor's id & table name
		predictor.setId(++id);
		predictor.setOutputTable(setting.predictorPrefix + predictor.getId());

		// Set predictor's names
		predictor.setName(predictor.getNameOnce(setting));
		predictor.setLongName(predictor.getLongNameOnce());

		// In Hive all tables/columns have name in lowercase
		if("Hive SQL".equals(setting.databaseVendor)) {
			predictor.setName(predictor.getName().toLowerCase());
			predictor.setLongName(predictor.getLongName().toLowerCase());
		}

		// Convert pattern to SQL
		predictor.setSql(setting.dialect.getPredictor(setting, predictor));

		// Set timestamp_build
		predictor.setTimestampBuilt(LocalDateTime.now());

		// Execute the SQL
		try {
			predictor.setOk(Network.executeUpdate(setting.dataSource, predictor.getSql(), setting.secondMax));
		} catch (SQLException e) {
			predictor.setException(e);
		}

		// If the execution failed, stop.
		if (!predictor.isOk()) return;

		// Add Primary Key constrain.
		// This is not because of speeding things up (indeed it has a negative impact on total runtime because only
		// a small proportion of the predictors gets into MainSample) but because it validates uniqueness of the tuples.
		// Azure and Teradata require not-null constraint -> skip it for MSSQL and Teradata and Hive.
		if (!"Microsoft SQL Server".equals(setting.databaseVendor) && !"Teradata".equals(setting.databaseVendor) && !"Hive SQL".equals(setting.databaseVendor)) {
			if (!setting.dialect.setPrimaryKey(setting, predictor.getOutputTable())) {
				logger.warn("Primary key constraint failed");
				return;
			}
		}

		// Add row count
		predictor.setRowCount(setting.dialect.getRowCount(setting, setting.outputSchema, predictor.getOutputTable()));
		if (predictor.getRowCount()==0) return;
		if (predictor.getRowCount()>maxRowLimit) {
			logger.warn("Predictor " + predictor.getName() + " has " + predictor.getRowCount() + " rows. But base table has only " + maxRowLimit + " rows.");
			return;
		}

		// Add null count
		predictor.setNullCount(predictor.getRowCount() - setting.dialect.getNotNullCount(setting, setting.outputSchema, predictor.getOutputTable(), predictor.getName()));
		if (predictor.getNullCount()==predictor.getRowCount()) return;

		// Get the predictor's data type
		getPredictorType(setting, predictor);

		// Calculate Chi2+conceptDrift for each targetColumn
		predictor.setRelevanceObject(Relevance.calculate(setting, predictor));
	}


	// Subroutine 8: Create predictor with index and QC.
	// CHANGES: skipping SQL creation
	// HAVE TO EXTRACT A METHOD or something...
	private static void materializePredictor2(Setting setting, Predictor predictor, int maxRowLimit) {

		// Set predictor's id & table name
//		predictor.setId(journal.getNextId(setting));
//		predictor.setOutputTable(setting.predictorPrefix + predictor.getId());
//
//		// Set predictor's names
//		predictor.setName(predictor.getNameOnce(setting));
//		predictor.setLongName(predictor.getLongNameOnce());
//
//		// Set default relevance value for the target.
//		predictor.setRelevance(setting.baseTarget, 0.0);
//
//		// Convert pattern to SQL
//		predictor.setSql(setting.dialect.getPredictor(setting, predictor));

		// Set timestamp_build
		predictor.setTimestampBuilt(LocalDateTime.now());

		// Execute the SQL
		try {
			predictor.setOk(Network.executeUpdate(setting.dataSource, predictor.getSql(), setting.secondMax));
		} catch (SQLException e) {
			predictor.setException(e);
		}

		// If the execution failed, stop.
		if (!predictor.isOk()) return;

		// Add Primary Key constrain.
		// This is not because of speeding things up (indeed it has a negative impact on total runtime because only
		// a small proportion of the predictors gets into MainSample) but because it validates uniqueness of the tuples.
		// Azure and Teradata require not-null constraint -> skip it for MSSQL and Teradata.
		if (!"Microsoft SQL Server".equals(setting.databaseVendor) && !"Teradata".equals(setting.databaseVendor) && !"Hive SQL".equals(setting.databaseVendor)) {
			if (!setting.dialect.setPrimaryKey(setting, predictor.getOutputTable())) {
				logger.warn("Primary key constraint failed");
				return;
			}
		}

		// Add row count
		predictor.setRowCount(setting.dialect.getRowCount(setting, setting.outputSchema, predictor.getOutputTable()));
		if (predictor.getRowCount()==0) return;
		if (predictor.getRowCount()>maxRowLimit) {
			logger.warn("Predictor " + predictor.getName() + " has " + predictor.getRowCount() + " rows. But base table has only " + maxRowLimit + " rows.");
			return;
		}

		// Add null count
		predictor.setNullCount(predictor.getRowCount() - setting.dialect.getNotNullCount(setting, setting.outputSchema, predictor.getOutputTable(), predictor.getName()));
		if (predictor.getNullCount()==predictor.getRowCount()) return;

		// Get the predictor's data type
		getPredictorType(setting, predictor);

		// Calculate Chi2+conceptDrift for each targetColumn
		predictor.setRelevanceObject(Relevance.calculate(setting, predictor));
	}


	// Sub-Subroutine: Reflect settings in columnMap & parameterMap into SQL
	protected static void addSQL(Predictor predictor, String sql) {
		for (String parameterName : predictor.getParameterMap().keySet()) {
			// Only whole words! Otherwise "@column" would also match "@name".
			String oldString = parameterName.substring(1); // remove the at-sign as it is not considered as a part of word in regex
			String newString = predictor.getParameterMap().get(parameterName);
			sql = sql.replaceAll("@\\b" + oldString + "\\b", newString);
		}
		predictor.setSql(sql);
	}

	// Subroutine: Is the predictor a string or a number? Just ask the database.
	// The answer is returned by modifying Predictor's fields.
	// The result is then used in getChi2() and getConceptDrift().
	// Note: The data type could be predicted from the pattern and pattern parameters. But the implemented
	// method is foolproof, though slow.
	private static void getPredictorType(Setting setting, Predictor predictor) {

		Table table = new Table();
		table.columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, predictor.getOutputTable());
		table.categorizeColumns(setting);

		Column column = table.getColumn(predictor.getName());

		predictor.setDataType(column.dataType);
		predictor.setDataTypeName(column.dataTypeName);

		// NOTE: WE DO NOT WANT STATISTICAL TYPE, WE WANT RAW TYPE!
		if (column.dataType == -7 || column.dataType == 16) {   // We treat here bit/boolean data type as nominal, not numerical!
			predictor.setDataTypeCategory("nominal");
		} else if (column.isTemporal) {
			predictor.setDataTypeCategory("temporal");
		} else if (column.isNumerical) {
			predictor.setDataTypeCategory("numerical");
		} else {
			predictor.setDataTypeCategory("nominal");
		}
	}

	private static String getColumnStatisticalType(String keyValue) {
		if (keyValue.contains("nominal")) return "nominal";
		if (keyValue.contains("numerical")) return "numerical";
		if (keyValue.contains("temporal")) return "temporal";
		String type = keyValue.substring(1, keyValue.length()-6);
		logger.warn("Unknown type encountered: " + type);
		return type;
	}
}
