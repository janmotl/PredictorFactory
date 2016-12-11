package featureExtraction;


import metaInformation.MetaOutput.OutputTable;
import org.apache.commons.lang3.text.WordUtils;
import run.Setting;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;


public class Predictor implements Comparable<Predictor> {

    // Private
    private int id;                                 // Unique number. SHOULD BE FINAL.
    private int groupId;                            // Id of the optimisation group
    private String sql;                             // SQL code
    private boolean isOk;                           // Summary flag for quality control
    private int rowCount;                           // Count of rows in the predictor's table
    private int nullCount;                          // Count of null rows in the predictor's table
    private String name;                            // Predictor's name abbreviated to comply with vendor's limits
    private String longName;                        // Predictor's name in it's whole glory
    private final LocalDateTime timestampDesigned;  // When the predictor was defined
    private LocalDateTime timestampBuilt;           // When the predictor was translated to SQL
    private LocalDateTime timestampDelivered;       // When the predictor was calculated by the database
    private SortedMap<String, String> parameterMap = new TreeMap<>();   // Map of values used in SQL generation
    private boolean isInferiorDuplicate = false;    // Decided based on the predictor's values
    private String duplicateName;                   // The name of the duplicate predictor
    private int candidateState = 1;                 // The states are: {1=candidate, 0=dropped, -1=toDrop}
    private String outputTable;                     // The name of the constructed predictor table
    private SortedMap<String, String> columnMap = new TreeMap<>();   // Contains {@nominalColumn=gender,...}
    private String rawDataType;                     // Not a statistical type. Contains {Nominal, Numerical, Temporal}

    // Note: Could have used Table.Column or OutputTable.Column for storage...
    private int dataType;                           // Data type as defined by JDBC
    private String dataTypeName;                    // Data type name as defined by database

    // Composition
    private OutputTable table;                      // The table with all the columns
    private final Pattern pattern;                  // The used pattern

    // Relevance of the predictor for classification
    // SHOULD BE AN OBJECT AND CONTAIN: Target, MeasureType, Value
    // FOR INSPIRATION HOW TO DEAL WITH A MIXTURE OF MEASURES WHERE WE MAXIMIZE AND MINIMIZE A VALUE SEE RAPIDMINER
    private SortedMap<String, Double> relevance = new TreeMap<>();
    private SortedMap<String, Double> conceptDrift = new TreeMap<>();

    // Constructor
    Predictor(Pattern p){
        if (p == null) {
            throw new NullPointerException("Pattern is null");
        }
        if (p.name == null) {
            throw new NullPointerException("Name in the pattern is null");
        }
        if (p.dialectCode == null) {
            throw new NullPointerException("DialectCode in the pattern is null");
        }
        if (p.author == null) {
            throw new NullPointerException("Author in the pattern is null");
        }
        if (p.cardinality == null) {
            throw new NullPointerException("Cardinality in the pattern is null");
        }
        if (p.dialectParameter == null) {
            throw new NullPointerException("DialectParameter in the pattern is null");
        }
        if (p.optimizeParameter == null) {
            throw new NullPointerException("OptimizeParameter in the pattern is null");
        }
        if (p.description == null) {
            throw new NullPointerException("Description in the pattern is null");
        }
        
        timestampDesigned = LocalDateTime.now();
        pattern = p;
    }


    // Copy constructor
    // Note that it merely performs a shallow copy of {OutputTable, Pattern} and it copies a few other attributes
    // that are required for correct functionality of Aggregation. Shallow copies are alright as it saves RAM
    // and we do not need (or want) to change anything in these objects.
    // Note also that com.rits.cloning.Cloner was failing on cloning Table.Column.uniqueValueSet with StackOverflow
    // error.
    protected Predictor(Predictor other) {
        this.groupId = other.groupId;                           // Required copy of the int
        this.sql = other.sql;                                   // Required copy of the String
        this.table = other.table;                               // Just a copy of the pointer is OK
        this.pattern = other.pattern;                           // Just a copy of the pointer is OK
        this.timestampDesigned = LocalDateTime.now();           // Newly created
        this.parameterMap = new TreeMap<>(other.parameterMap);  // Shallow copy of the Map is OK
        this.columnMap = new TreeMap<>(other.columnMap);        // Shallow copy of the Map is OK

        // TO DELETE
//        this.id = other.id;
//        this.outputTable = other.outputTable;
//        this.name = other.name;
//        this.longName = other.longName;
//        this.timestampBuilt = other.timestampBuilt;
//        this.timestampDelivered = other.timestampDelivered;
//        this.isOk = other.isOk;
//        this.rowCount = other.rowCount;
//        this.nullCount = other.nullCount;
//        this.isInferiorDuplicate = other.isInferiorDuplicate;
//        this.duplicateName = other.duplicateName;
//        this.candidateState = other.candidateState;
//        this.rawDataType = other.rawDataType;
//        this.dataType = other.dataType;
//        this.dataTypeName = other.dataTypeName;
//        this.relevance = other.relevance;
//        this.conceptDrift = other.conceptDrift;
    }

    // Get column name.
    // Note that the returned String can be longer than the allowed length in the database.
    // For example PostgreSQL limits column labels to 63 characters.
    // Firebird limits the length to 31 characters.
    // The expected result is something like: Path_Table_Columns_Pattern_Parameters...
    // Consider switching to StringBuffer
    // The time consuming code should be performed just once and stored 
    public String getLongNameOnce() {
        // Path
        String name = "";
        for (int i = 1; i < getPropagationPath().size(); i++) {  // Ignore the first table, base table, as it is fixed.
            name = name + getPropagationPath().get(i) + "__"; // Use doubled separators to deal with already underscored entities.
        }
        
        // Add table name
        name = name + getOriginalTable();
        
        // Add column names
        name = name + "_";  // Three separators to separate tables from columns
        for (String columnName : getColumnMap().values()) {
            name = name + "__" + columnName;
        }

        // Replace special characters with spaces
        String patternName = pattern.name.replaceAll("[^a-zA-Z0-9_\\s]", " ");
                
        // Convert the string to camelCase and remove the spaces 
        patternName = WordUtils.capitalizeFully(patternName, new char[]{' '}).replaceAll(" ", "");
        
        // We would like to have camelCase, not CamelCase
        patternName = Character.toLowerCase(patternName.charAt(0)) + patternName.substring(1);
        
        name = name + "___" + patternName;
        
        // Add parameters. Ignore all special symbols (particularly "@" from "@column" parameters)
        for (String parameter : parameterMap.keySet()) {
            name = name + "__" + parameterMap.get(parameter).replaceAll(" ", "").replaceAll("[^a-zA-Z0-9_]", "");
        }
                    
        return name;
    }
    
    // SHOULD INTRODUCE A SHORTER VARIANT IF identifierLengthMax is small: column_pattern_parameter.
    public String getNameOnce(Setting setting) {
        int identifierLengthMax = setting.identifierLengthMax;
        // 3 characters are reserved for underscores, 6 characters are for id. Divide into 3 parts (table, column, parameter)
        int length = (int) Math.floor((identifierLengthMax-9)/3);
        
        // Add table name
        String name = getOriginalTable().substring(0, Math.min(getOriginalTable().length(), length));
        
        // Add column names
        for (String columnName : getColumnMap().values()) {
            name = name + "_" + columnName;
        }
        
        name = name.substring(0, Math.min(name.length(), 2*length+1));

        // Replace special characters with spaces
        String patternName = pattern.name.replaceAll("[^a-zA-Z0-9_\\s]", " ");
                
        // Convert the string to camelCase and remove the spaces 
        patternName = WordUtils.capitalizeFully(patternName, new char[]{' '}).replaceAll(" ", "");
        
        // We would like to have camelCase, not CamelCase
        patternName = Character.toLowerCase(patternName.charAt(0)) + patternName.substring(1);
        
        name = name + "_" + patternName;
        
        // Add parameters. Ignore all special symbols (particularly "@" from "@column" parameters)
        for (String parameter : parameterMap.keySet()) {
            name = name + "_" + parameterMap.get(parameter).replaceAll(" ", "").replaceAll("[^a-zA-Z0-9_]", "");
        }

        name = name.substring(0, Math.min(name.length(), 3 * length + 2));
                
        // Add id with zero padding from left
        name = name + "_" +id;
            
        return name;
    }

    
    
    ////////////// Comparators ////////////////

    // Predictors are sorted by their id in collections like SortedSet
    @Override
    public int compareTo(Predictor anotherPredictor) {
        int anotherPredictorId = anotherPredictor.getId();
        return id - anotherPredictorId;
    }

    // Sort first by candidateState in descending order.
    // Then sort by relevance in descending order (higher values are better).
    // If relevance is calculated for multiple targets, compare based on the maximum relevance.
    // NOTE: This is not ideal - what if prediction of some target is tougher than prediction of another target?
    // NOTE: I should have journalTopN for each target and produce mainSample for each target!
    // In case of tie, sort by runtime in ascending order (smaller values are better).
    // NOTE: HAVE TO TEST ON NULLS AND EMPTY RELEVANCE MAPS
    public static final Comparator<Predictor> RelevanceComparator = new Comparator<Predictor>(){
        @Override
        public int compare(Predictor o1, Predictor o2) {

            // Candidate states (predictors that are not OK or are duplicate have candidateState<1)
            if (o1.getCandidateState() > o2.getCandidateState()) {
                return -1;
            } else if (o1.getCandidateState() < o2.getCandidateState()) {
                return 1;
            }

            // Get maximum relevance, if necessary
            Double relevance1 = Collections.max(o1.getRelevanceMap().values());
            Double relevance2 = Collections.max(o2.getRelevanceMap().values());

            // Compare based on the maximum relevance
            if (relevance1.compareTo(relevance2) > 0) {
                return -1;
            } else if (relevance1.compareTo(relevance2) < 0) {
                return 1;
            }

            // Compare based on the runtime, if necessary
            if (o1.getRuntime() < o2.getRuntime()) {
                return -1;
            } else if (o1.getRuntime() > o2.getRuntime()) {
                return 1;
            }
            return 0;
        }
    };



    /////////// To string ///////////
    @Override
    public String toString() {
        return getLongNameOnce() + " " + Collections.max(relevance.values()) + " " + candidateState;
    }
    
    
    
    /////////// Convenience getters and setters //////////
    public void setParameter(String key, String value) {
        parameterMap.put(key, value);
    }
    
    public Double getRelevance(String target) {
        return relevance.get(target);
    }
    
    public void setRelevance(String target, Double value) {
        relevance.put(target, value);
    }

    public Double getConceptDrift(String target) {
        return conceptDrift.get(target);
    }

    public void setConceptDrift(String target, Double value) {
        conceptDrift.put(target, value);
    }


    public double getRuntime() {
        // Runtime in seconds with three decimal values. Assumes that start time and end time are available.
        return timestampBuilt.until(timestampDelivered, ChronoUnit.MILLIS)/1000.0;
    }



    /////////// Generic setters and getters /////////////
    // Too many of them. Consider lombok OR VALJOGen OR Scala OR Groovy OR AutoValue OR Immutables OR use global parameters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getGroupId() {
        return groupId;
    }

    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    public LocalDateTime getTimestampBuilt() {
        return timestampBuilt;
    }

    public void setTimestampBuilt(LocalDateTime timestampBuilt) {
        this.timestampBuilt = timestampBuilt;
    }

    public LocalDateTime getTimestampDelivered() {
        return timestampDelivered;
    }

    public void setTimestampDelivered(LocalDateTime timestampDelivered) {
        this.timestampDelivered = timestampDelivered;
    }

    public LocalDateTime getTimestampDesigned() {
        return timestampDesigned;
    }

    public SortedMap<String, String> getParameterMap() {
        return parameterMap;
    }

    public String getPatternCode() {
        return pattern.dialectCode;
    }
    
    public String getPatternName() {
        return pattern.name;
    }

    public String getPatternAuthor() {
        return pattern.author;
    }
    
    public String getPatternCardinality() {
        return pattern.cardinality;
    }

    public SortedMap<String, String> getPatternParameterMap() {
        return pattern.dialectParameter;
    }

    public List<Pattern.OptimizeParameters> getPatternOptimizeParameter() {
        return pattern.optimizeParameter;
    }

    public String getPatternDescription() {
        return pattern.description;
    }

    public Boolean getPatternRequiresBaseDate() {
        return pattern.requiresBaseDate;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getLongName() {
        return longName;
    }

    public void setLongName(String longName) {
        this.longName = longName;
    }
    
    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }


    public boolean isOk() {
        return isOk;
    }

    public void setOk(boolean isOk) {
        this.isOk = isOk;
    }
    
    public int getRowCount() {
        return rowCount;
    }
    
    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }
    
    public int getNullCount() {
        return nullCount;
    }

    public void setNullCount(int nullCount) {
        this.nullCount = nullCount;
    }
    
    public SortedMap<String, Double> getRelevanceMap() {
        return relevance;
    }

    public boolean isInferiorDuplicate() {
        return isInferiorDuplicate;
    }

    public void setInferiorDuplicate(boolean inferiorDuplicate) {
        isInferiorDuplicate = inferiorDuplicate;
    }

    public String getDuplicateName() {
        return duplicateName;
    }

    public void setDuplicateName(String duplicateName) {
        this.duplicateName = duplicateName;
    }

    public int getCandidateState() {
        return candidateState;
    }

    public void setCandidateState(int candidateState) {
        this.candidateState = candidateState;
    }


    public OutputTable getTable() {
        return table;
    }

    public void setTable(OutputTable table) {
        this.table = table;
    }


    public String getPropagatedTable() {
        return table.name;
    }

    public String getOriginalTable() {
        return table.originalName;
    }

    public String getPropagationDate() {
        return table.temporalConstraint;
    }

    public List<String> getPropagationPath() {
        return table.propagationPath;
    }

    public String getOutputTable() {
        return outputTable;
    }

    public void setOutputTable(String outputTable) {
        this.outputTable = outputTable;
    }

    public SortedMap<String, String> getColumnMap() {
        return columnMap;
    }

    public void setColumnMap(SortedMap<String, String> columnMap) {
        this.columnMap = columnMap;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }

    public void setDataTypeName(String dataTypeName) {
        this.dataTypeName = dataTypeName;
    }

    public int getDataType() {
        return dataType;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public String getRawDataType() {
        return rawDataType;
    }

    public void setRawDataType(String rawDataType) {
        this.rawDataType = rawDataType;
    }
}