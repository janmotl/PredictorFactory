package featureExtraction;

import connection.Network;
import connection.SQL;
import featureExtraction.Pattern.OptimizeParameters;
import metaInformation.Column;
import metaInformation.MetaOutput.OutputTable;
import metaInformation.StatisticalType;
import metaInformation.Table;
import org.apache.log4j.Logger;
import run.Setting;
import utility.BlackWhiteList;
import utility.Meta;
import utility.PatternMap;
import utility.TextParser;

import java.time.LocalDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class Aggregation {
    // Logging
    private static final Logger logger = Logger.getLogger(Aggregation.class.getName());


    // Do it
    public static Journal aggregate(Setting setting, SortedMap<String, OutputTable> tableMetadata ) {
    
        // Initialization
        Journal journal;    // Log of all predictors
        int groupId = 1;    // To group different refinements together

    
        // 1) Get predictors
        List<Predictor> predictorList = loopPatterns(setting);
        // Log patterns
        setting.dialect.getJournalPattern(setting);
        setting.dialect.addToJournalPattern(setting, predictorList);
    
        // 2) Set @targetValue
            // First, get the cached list of unique values in the target column
            Set<String> uniqueValueSet = new HashSet<>();
            for (OutputTable table : tableMetadata.values()) {
                if (setting.targetTable.equals(table.originalName)) {
                    uniqueValueSet = table.getColumn(setting.targetColumn).uniqueValueSet;
                    break; // No need to continue
                }
            }

            // Second, Loop over all the predictors
            List<Predictor> predictorList2 = new ArrayList<>();
            for (Predictor predictor : predictorList) {
                predictorList2.addAll(addTargetValue(setting, predictor, uniqueValueSet));
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
            predictorList6.addAll(addValue(predictor));
        }
    
        // 7) Optimize parameters
        List<Predictor> predictorList7 = new ArrayList<>();
        for (Predictor predictor : predictorList6) {
            predictorList7.addAll(optimizeAll(predictor, groupId));
            groupId++; // GroupId is unique per optimization group
        }
    

        // 8) Execute the SQL & log the result
        int maxRowLimit = setting.dialect.getRowCount(setting, setting.outputSchema, setting.baseSampled); // For QC purposes.

        journal = new Journal(setting, predictorList7.size());
        logger.debug(journal.getExpectedPredictorCount() + " predictors are scheduled for calculation.");

        for (Predictor predictor : predictorList7) {
            materializePredictor(setting, journal, predictor, maxRowLimit);
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
    protected static List<Predictor> addTargetValue(Setting setting, Predictor predictor, Set<String> uniqueValueSet){

        boolean isClassification = "classification".equals(setting.task);
        boolean containsTargetValue = predictor.getPatternCode().contains("@targetValue");

        // If the pattern uses target value...
        if (containsTargetValue) {
            if (isClassification) {
                predictor.setParameter("@targetValue", uniqueValueSet.iterator().next()); // Just some random item
            } else {
                return new ArrayList<>();
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
        List<Predictor> predictorList = new ArrayList<>();
        predictorList.add(predictor);
    
        // Loop over each parameter
        for (Entry<String, String> parameter : predictor.getPatternParameterMap().entrySet()) {
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
    protected static List<Predictor> loopTables(Predictor predictor, SortedMap<String, OutputTable> tableMetadata) {
    
        // Initialize the output
        List<Predictor> outputList = new ArrayList<>();
    
        // For each propagated table.
        for (OutputTable workingTable : tableMetadata.values()) {

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
    // NO GUARANTIES ABOUT COLUMN1 != COLUMN2 IF THEY ARE OF THE SAME TYPE!
    // NEITHER THAT {COLUMN1, COLUMN2} WILL BE REPEATED AS {COLUMN2, COLUMN1}!
    // CHECK HOW DO I TREAT COLUMNS THAT ARE BOTH, NUMERICAL AND NOMINAL...
    protected static List<Predictor> loopColumns(Setting setting, Predictor predictor) {
    
        // Initialize the output
        List<Predictor> predictorList = new ArrayList<>();
        predictorList.add(predictor);
    
        // Find each occurrence of "@?*column*" in SQL. The search is case sensitive (to force nicely formatted patterns).
        String regexPattern = "@\\w+Column\\w*";
        Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(predictor.getSql());
        while (m.find()) {
            predictor.getColumnMap().put(m.group(), null);
        }
    
        // Loop over each column in the predictor
        for (Entry<String, String> parameter : predictor.getColumnMap().entrySet()) {
            predictorList = expandColumn(setting, predictorList, parameter.getKey(), predictor.getTable());
        }
    
        return predictorList;
    }

    private static List<Predictor> expandColumn(Setting setting, List<Predictor> predictorList, String columnKey, OutputTable table) {
    
        List<Predictor> outputList = new ArrayList<>();
    
        for (Predictor predictor : predictorList) {
            // Select columns from the table with the correct data type. 
            SortedSet<Column> columnValueSet = new TreeSet<>();
        
            // Matches: Case insensitive plus suffix with any number of digits
            if (columnKey.matches("(?i)@NUMERICALCOLUMN\\d*")) columnValueSet = table.getColumns(setting, StatisticalType.NUMERICAL);
            else if (columnKey.matches("(?i)@NOMINALCOLUMN\\d*")) columnValueSet = table.getColumns(setting, StatisticalType.NOMINAL);
            else if (columnKey.matches("(?i)@TIMECOLUMN\\d*")) columnValueSet = table.getColumns(setting, StatisticalType.TEMPORAL);
            else if (columnKey.matches("(?i)@IDCOLUMN\\d*")) columnValueSet = table.getColumns(setting, StatisticalType.ID);
            else logger.warn("The term: " + columnKey +  " in pattern: " + predictor.getPatternName() + " is not recognized as a valid column identifier. Expected terms are: {numericalColumn, nominalColumn, timeColumn, idColumn}.");
        
            // Bind the columnKey to the actual columnValue.    
            for (Column columnValue : columnValueSet) {
                Predictor cloned = new Predictor(predictor);
                cloned.getColumnMap().put(columnKey, columnValue.name);
                outputList.add(cloned);
            }
        }
    
        return outputList;
    }


    // Subroutine 6: Populate @value parameter.
    // NOTE: The value is populated only for the first column in the columnMap.
    protected static List<Predictor> addValue(Predictor predictor) {
    
        // Initialize the output
        List<Predictor> predictorList = new ArrayList<>();

        // If the pattern code contains @value
        if (predictor.getPatternCode().contains("@value")) {
            for (Entry<String, String> column : predictor.getColumnMap().entrySet()) {
                // Pick a nominal column
                if (column.getKey().toUpperCase().matches("@NOMINALCOLUMN\\d*")) {
                
                    // Get list of unique values
                    Set<String> valueList = predictor.getTable().getColumn(column.getValue()).uniqueValueSet;

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
        List<Predictor> outputList = new ArrayList<>();
        outputList.add(predictor);
    
        // Loop over all parameters to optimize
        for (OptimizeParameters parameter : predictor.getPatternOptimizeParameter()) {
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
        List<Predictor> outputList = new ArrayList<>();
    
        for (Predictor predictor : predictorList) {
            // CURRENTLY JUST SWEEP OVER THE VALUES. USE A REAL OPTIMISATION TOOLBOX.
            for (int i = 0; i < parameter.iterationLimit; i++) {
                double value = parameter.min + i * (parameter.max-parameter.min)/(parameter.iterationLimit-1);
                Predictor clonedPredictor = new Predictor(predictor);
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
        predictor.setOutputTable(setting.predictorPrefix + (predictor.getId()));
    
        // Set predictor's names
        predictor.setName(predictor.getNameOnce(setting));
        predictor.setLongName(predictor.getLongNameOnce());
    
        // Set default relevance value for the target.
        predictor.setRelevance(setting.baseTarget, 0.0);
    
        // Convert pattern to SQL
        predictor.setSql(setting.dialect.getPredictor(setting, predictor));
    
        // Set timestamp_build
        predictor.setTimestampBuilt(LocalDateTime.now());
    
        // Execute the SQL
        predictor.setOk(Network.executeUpdate(setting.dataSource, predictor.getSql()));
    
        // If the execution failed, stop.
        if (!predictor.isOk()) return;
    
        // Add Primary Key constrain.
        // This is not because of speeding things up (indeed it has a negative impact on total runtime because only
        // a small proportion of the predictors gets into MainSample) but because it validates uniqueness of the tuples.
        // Azure requires not-null constraint -> skip it for MSSQL.
        if (!"Microsoft SQL Server".equals(setting.databaseVendor)) {
            if (!setting.dialect.setPrimaryKey(setting, predictor.getOutputTable())) {
                logger.warn("Primary key constrain failed");
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

        // Add univariate relevance estimate
        if ("classification".equalsIgnoreCase(setting.task)) {
            predictor.setRelevance(setting.baseTarget, setting.dialect.getChi2(setting, predictor));
        } else {
            predictor.setRelevance(setting.baseTarget, setting.dialect.getR2(setting, predictor));
        }

        // Add concept drift (so far just for nominal labels & if targetDate exists)
        if ("classification".equalsIgnoreCase(setting.task) && setting.targetDate!=null) {
            predictor.setConceptDrift(setting.baseTarget, setting.dialect.getConceptDrift(setting, predictor));
        }

    }


    // Sub-Subroutine: Reflect settings in columnMap & parameterMap into SQL
    protected static Predictor addSQL(Predictor predictor, String sql) {
    
        for (String parameterName : predictor.getParameterMap().keySet()) {
            // Only whole words! Otherwise "@column" would also match "@name".
            String oldString = parameterName.substring(1); // remove the at-sign as it is not considered as a part of word in regex
            String newString = predictor.getParameterMap().get(parameterName);
            sql = sql.replaceAll("@\\b" + oldString + "\\b", newString);
        }
        predictor.setSql(sql);
       
        return predictor;
    }

    // Subroutine: Is the predictor a string or a number? Just ask the database.
    // The answer is returned by modifying Predictor's fields.
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
        if (column.dataType == -7) {   // We treat here boolean data type as nominal, not numerical!
            predictor.setRawDataType("nominal");
        } else if (column.isTemporal) {
            predictor.setRawDataType("temporal");
        } else if (column.isNumerical) {
            predictor.setRawDataType("numerical");
        }

        predictor.setRawDataType("nominal");
    }

}
