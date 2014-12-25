package run;


import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.lang3.text.WordUtils;


public class Predictor implements Comparable<Predictor> {

	// Struct for predictor's metadata
	public String outputTable;
	public String inputTable;
	public String inputTableOriginal;	// The table name before propagation. Useful for predictor naming. And origin tracking.
	public SortedMap<String, String> columnMap;	// Contains columns like {@numericalColumn1, @nominalColumn3,...}
	public String propagationDate;			// The single column name that was used during base propagation as time constrain.
	public List<String> propagationPath;	// In the case of loops path makes difference   
	
	//public String parameter1;
	protected Map<String, String> parameterList = new HashMap<String, String>();
	
	// Relevance of the predictor for classification
	// SHOULD CONTAIN: Target, MeasureType, Value
	private Map<String, Double> relevanceList = new HashMap<String, Double>(); 
	
		
	// Private
	private int id; 								// Unique number. SHOULD BE FINAL.
	private String sql;								// SQL code
	private boolean isOk;							// Flag 
	private int rowCount;							// Count of rows in the predictor's table
	private final String patternName;				// Inherited from the pattern during the class construction
	private final String patternCode;				// Inherited from the pattern during the class construction
	private final LocalDateTime timestampDesigned; 	// When the predictor was defined
	private LocalDateTime timestampBuilt; 			// When the predictor was translated to SQL
	private LocalDateTime timestampDelivered; 		// When the predictor was calculated by the database
	
	// Constructor
	Predictor(Pattern pattern){
		timestampDesigned = LocalDateTime.now();
		patternName = pattern.getName();
		patternCode = pattern.getCode();
	}

	
	// Get column name.
	// Note that the returned String can be longer than the allowed length in the database.
	// For example PostgreSQL limits column labels to 63 characters.
	// Firebird limits the length to 31 characters.
	// Consider switching to StringBuffer
	// The time consuming code should be performed just once and stored
	public String getName() {
		
		// The expected result is something like: Table_Columns_Pattern_Parameters...
		String name = inputTableOriginal;	// Use propagated name without "propagated" prefix
		name = name.replaceFirst("propagated_", "");
		
		for (String columnName : columnMap.values()) {
			name = name + "_" + columnName;
		}

		// Remove spaces from pattern name and convert the string to camelCase
		String pattern = WordUtils.capitalizeFully(patternName, new char[]{' '}).replaceAll(" ", "");
		
		// Keep only alphanumeric characters and underscore
		pattern = pattern.replaceAll("[^a-zA-Z0-9_]", "");
		
		// We would like to have camelCase, not CamelCase
		pattern = Character.toLowerCase(pattern.charAt(0)) + pattern.substring(1);
		
		name = name + "_" + pattern;
		
		// Add parameters
		for (String parameter : parameterList.keySet()) {
			name = name + "_" + parameterList.get(parameter).replaceAll(" ", "");
		}
					
		return name;
	}

	
	// We want to be able to compare the predictors by their id
	@Override
	public int compareTo(Predictor anotherPredictor) {
	    int anotherPredictorId = anotherPredictor.getId();  
	    return id - anotherPredictorId;    
	  }
	
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
	
	public Map<String, Double> getRelevanceList() {
		return relevanceList;
	}
	
	public void setRelevanceList(Map<String, Double> relevanceList) {
		this.relevanceList = relevanceList;
	}


	

	

	


	
	
}