package featureExtraction;


import org.apache.commons.lang3.text.WordUtils;
import run.Setting;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;


public class Predictor implements Comparable<Predictor> {

    // SHOULD UNIFY - PUBLIC AND PRIVATE looks ugly.
    // Should be a composite of {pattern, table, column} and provide relevant functionality of these classes.
    // Struct for predictor's metadata. Make sure that each collection is initialized to something and doesn't return null!
    public String outputTable;          // The name of the constructed predictor table.
    public String propagatedTable;      // The input table name after propagation.
    public String originalTable;        // The input table name before propagation. Useful for origin tracking.
    public String propagationDate;      // The single column name that was used during base propagation as time constrain.
    public List<String> propagationPath = new ArrayList<>();    // In the case of loops path makes difference
    public SortedMap<String, String> columnMap = new TreeMap<>();   // Contains {@nominalColumn=gender,...}
    
    
    // Relevance of the predictor for classification
    // SHOULD BE AN OBJECT AND CONTAIN: Target, MeasureType, Value
    // FOR INSPIRATION HOW TO DEAL WITH A MIXTURE OF MEASURES WHERE WE MAXIMIZE AND MINIMIZE A VALUE SEE RAPIDMINER
    private SortedMap<String, Double> relevance = new TreeMap<>();
    
        
    // Private
    private int id;                                 // Unique number. SHOULD BE FINAL.
    private int groupId;                            // Id of the optimisation group
    private String sql;                             // SQL code
    private boolean isOk;                           // Summary flag for quality control
    private int rowCount;                           // Count of rows in the predictor's table
    private int nullCount;                          // Count of null rows in the predictor's table
    private final int patternTopN;                  // Inherited from the pattern
    private final String patternName;               // Inherited from the pattern during the class construction
    private final String patternAuthor;             // Adds an element of gamification
    private final String patternCode;               // Inherited from the pattern
    private final String patternCardinality;        // Inherited from the pattern
    private final String patternDescription;        // Inherited from the pattern
    private final Boolean patternRequiresBaseDate;  // Inherited from the pattern
    private final SortedMap<String, String> patternParameterMap;    // Inherited from the pattern
    private final List<Pattern.OptimizeParameters> patternOptimizeList; // Inherited from the pattern
    private String name;                            // Predictor's name abbreviated to comply with vendor's limits
    private String longName;                        // Predictor's name in it's whole glory
    private final LocalDateTime timestampDesigned;  // When the predictor was defined
    private LocalDateTime timestampBuilt;           // When the predictor was translated to SQL
    private LocalDateTime timestampDelivered;       // When the predictor was calculated by the database
    private SortedMap<String, String> parameterMap = new TreeMap<>();   // Map of values used in SQL generation
    private boolean isInferiorDuplicate = false;    // Decided based on the predictor's values
    private String duplicateName;                   // The name of the duplicate predictor
    private int candidateState = 1;                 // The states are: {1=candidate, 0=dropped, -1=toDrop}
    
    
    // Constructor
    Predictor(Pattern pattern){
        if (pattern == null) {
            throw new NullPointerException("Pattern is null");
        }
        if (pattern.name == null) {
            throw new NullPointerException("Name in the pattern is null");
        }
        if (pattern.dialectCode == null) {
            throw new NullPointerException("DialectCode in the pattern is null");
        }
        if (pattern.author == null) {
            throw new NullPointerException("Author in the pattern is null");
        }
        if (pattern.cardinality == null) {
            throw new NullPointerException("Cardinality in the pattern is null");
        }
        if (pattern.dialectParameter == null) {
            throw new NullPointerException("DialectParameter in the pattern is null");
        }
        if (pattern.optimizeParameter == null) {
            throw new NullPointerException("OptimizeParameter in the pattern is null");
        }
        if (pattern.description == null) {
            throw new NullPointerException("Description in the pattern is null");
        }
        
        timestampDesigned = LocalDateTime.now();
        patternName = pattern.name;
        patternCode = pattern.dialectCode;
        patternAuthor = pattern.author; 
        patternCardinality = pattern.cardinality;
        patternRequiresBaseDate = pattern.requiresBaseDate;
        patternParameterMap = pattern.dialectParameter;
        patternOptimizeList = pattern.optimizeParameter;
        patternTopN = pattern.topN;
        patternDescription = pattern.description;
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
        for (int i = 1; i < propagationPath.size(); i++) {  // Ignore the first table, base table, as it is fixed.
            name = name + propagationPath.get(i) + "__"; // Use doubled separators to deal with already underscored entities.
        }
        
        // Add table name
        name = name + originalTable;
        
        // Add column names
        name = name + "_";  // Three separators to separate tables from columns
        for (String columnName : columnMap.values()) {
            name = name + "__" + columnName;
        }

        // Replace special characters with spaces
        String pattern = patternName.replaceAll("[^a-zA-Z0-9_\\s]", " ");
                
        // Convert the string to camelCase and remove the spaces 
        pattern = WordUtils.capitalizeFully(pattern, new char[]{' '}).replaceAll(" ", "");
        
        // We would like to have camelCase, not CamelCase
        pattern = Character.toLowerCase(pattern.charAt(0)) + pattern.substring(1);
        
        name = name + "___" + pattern;
        
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
        String name = originalTable.substring(0, Math.min(originalTable.length(), length));
        
        // Add column names
        for (String columnName : columnMap.values()) {
            name = name + "_" + columnName;
        }
        
        name = name.substring(0, Math.min(name.length(), 2*length+1));

        // Replace special characters with spaces
        String pattern = patternName.replaceAll("[^a-zA-Z0-9_\\s]", " ");
                
        // Convert the string to camelCase and remove the spaces 
        pattern = WordUtils.capitalizeFully(pattern, new char[]{' '}).replaceAll(" ", "");
        
        // We would like to have camelCase, not CamelCase
        pattern = Character.toLowerCase(pattern.charAt(0)) + pattern.substring(1);
        
        name = name + "_" + pattern;
        
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
        return patternCode;
    }
    
    public String getPatternName() {
        return patternName;
    }

    public String getPatternAuthor() {
        return patternAuthor;
    }
    
    public String getPatternCardinality() {
        return patternCardinality;
    }

    public SortedMap<String, String> getPatternParameterList() {
        return patternParameterMap;
    }

    public List<Pattern.OptimizeParameters> getPatternOptimizeList() {
        return patternOptimizeList;
    }
    
    public int getPatternTopN() {
        return patternTopN;
    }

    public String getPatternDescription() {
        return patternDescription;
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

    public Boolean getPatternRequiresBaseDate() {
        return patternRequiresBaseDate;
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
}