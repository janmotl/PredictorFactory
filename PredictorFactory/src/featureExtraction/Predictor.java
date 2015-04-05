package featureExtraction;


import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.text.WordUtils;

import run.Setting;


public class Predictor implements Comparable<Predictor> {

	// Struct for predictor's metadata. Make sure that each collection is initialized to something and doesn't return null!
	public String outputTable;			// The name of the constructed predictor table.
	public String propagatedTable;		// The input table name after propagation.
	public String originalTable;		// The input table name before propagation. Useful for origin tracking.
	public String propagationDate;		// The single column name that was used during base propagation as time constrain.
	public List<String> propagationPath = new ArrayList<String>();	// In the case of loops path makes difference
	public SortedMap<String, String> columnMap = new TreeMap<String, String>();	// Contains {@nominalColumn=gender,...}
	protected Map<String, String> parameterList = new HashMap<String, String>();
	
	// Relevance of the predictor for classification
	// SHOULD CONTAIN: Target, MeasureType, Value
	private SortedMap<String, Double> relevance = new TreeMap<String, Double>(); 
	
		
	// Private
	private int id; 								// Unique number. SHOULD BE FINAL.
	private String sql;								// SQL code
	private boolean isOk;							// Flag 
	private int rowCount;							// Count of rows in the predictor's table
	private int nullCount;							// Count of null rows in the predictor's table
	private final String patternName;				// Inherited from the pattern during the class construction
	private final String patternAuthor;				// Adds an element of gamification
	private final String patternCode;				// Inherited from the pattern
	private String name;							// Predictor's name abbreviated to comply with vendor's limits
	private String longName;						// Predictor's name in it's whole glory
	private final LocalDateTime timestampDesigned; 	// When the predictor was defined
	private LocalDateTime timestampBuilt; 			// When the predictor was translated to SQL
	private LocalDateTime timestampDelivered; 		// When the predictor was calculated by the database
	
	
	// Constructor
  	Predictor(Pattern pattern){
		timestampDesigned = LocalDateTime.now();
		patternName = pattern.name;
		patternCode = pattern.dialectCode;
		patternAuthor = pattern.author;	
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
		for (int i = 1; i < propagationPath.size(); i++) {	// Ignore the first table, base table, as it is fixed.
			name = name + propagationPath.get(i) + "_";
		}
		
		// Add table name
		name = name + originalTable;
		
		// Add column names
		for (String columnName : columnMap.values()) {
			name = name + "_" + columnName;
		}

		// Replace special characters with spaces
		String pattern = patternName.replaceAll("[^a-zA-Z0-9_\\s]", " ");
				
		// Convert the string to camelCase and remove the spaces 
		pattern = WordUtils.capitalizeFully(pattern, new char[]{' '}).replaceAll(" ", "");
		
		// We would like to have camelCase, not CamelCase
		pattern = Character.toLowerCase(pattern.charAt(0)) + pattern.substring(1);
		
		name = name + "_" + pattern;
		
		// Add parameters. Ignore all special symbols (particularly "@" from "@column" parameters)
		for (String parameter : parameterList.keySet()) {
			name = name + "_" + parameterList.get(parameter).replaceAll(" ", "").replaceAll("[^a-zA-Z0-9_]", "");
		}
					
		return name;
	}
	
	public String getNameOnce(Setting setting) {
		int indentifierLengthMax = setting.indentifierLengthMax;
		
		// 3 characters are reserved for underscores, 6 characters are for id.
		int length = (int) Math.floor((indentifierLengthMax-9)/3); 
		
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
		for (String parameter : parameterList.keySet()) {
			name = name + "_" + parameterList.get(parameter).replaceAll(" ", "").replaceAll("[^a-zA-Z0-9_]", "");
		}
		
		name = name.substring(0, Math.min(name.length(), 3*length+2));
		
		// Add id with zero padding from left
		name = name + "_" +id;
			
		return name;
	}

	
	// Predictors are sorted by their id in collections like SortedSet
	@Override
	public int compareTo(Predictor anotherPredictor) {
	    int anotherPredictorId = anotherPredictor.getId();  
	    return id - anotherPredictorId;    
	}
	  
    // Sometimes we may want to sort based on maximal relevance (for example when exporting the computed predictors)
    public static final Comparator<Predictor> RelevanceComparator = new Comparator<Predictor>(){
        @Override
        public int compare(Predictor o1, Predictor o2) {
            return Collections.max(o1.getRelevance().values()).compareTo(Collections.max(o2.getRelevance().values()));
        }
    };
    
    // For completeness also comparison by Id. Although I don't use it anywhere.
	public static final Comparator<Predictor> IdComparator = new Comparator<Predictor>(){
        @Override
        public int compare(Predictor o1, Predictor o2) {
            return o1.getId() - o2.getId();
        }
    };
		
	/////////// Generic setters and getters /////////////
	// Too many of them. Consider lombok OR VALJOGen OR Scala OR Groovy OR use global parameters
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
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

	public Map<String, String> getParameterList() {
		return parameterList;
	}

	public void setParameterList(Map<String, String> parameterList) {
		this.parameterList = parameterList;
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

	public SortedMap<String, Double> getRelevance() {
		return relevance;
	}
	
	public void setRelevance(SortedMap<String, Double> relevanceList) {
		this.relevance = relevanceList;
	}


	

	

	


	
	
}