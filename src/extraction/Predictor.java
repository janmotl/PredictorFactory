package extraction;


import extraction.Pattern.OptimizeParameters;
import meta.MetaOutput.OutputTable;
import org.apache.commons.lang3.text.WordUtils;
import run.Setting;

import javax.xml.bind.annotation.XmlType;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

@XmlType(name = "predictor")
public class Predictor implements Comparable<Predictor> {
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
	private Exception exception;                    // As thrown from the database
	private String baseTarget;                      // The single baseTarget column name for patterns like WOE
	private String targetColumn;                    // The single targetColumn (the original name) for reporting. For WOE. Null for aggregates
	private String dataTypeCategory;                // Not a statistical type. Contains {Nominal, Numerical, Temporal}

	// Note: Could have used Table.Column or OutputTable.Column for storage...
	private int dataType;                           // Data type as defined by JDBC
	private String dataTypeName;                    // Data type name as defined by database

	// Composition
	private OutputTable table;                      // The table with all the columns
	private final Pattern pattern;                  // The used pattern
	private Relevance relevance;                    // The storage for the measured predictor's relevance

	// Relevance of the predictor for classification
	private String chosenBaseTarget;                    // A single target (and relevance) based on which the predictor is ranked

	// Constructor
	public Predictor(Pattern p) {
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
		table = new OutputTable();
		relevance = new Relevance();
	}

	// Constructor for JAXB (as Journal is marshaled)
	public Predictor() {
		timestampDesigned = LocalDateTime.now(); // Will be overwritten as we are using field access to the variables.
		pattern = new Pattern();
		table = new OutputTable();
		relevance = new Relevance();
	}

	// Copy constructor
	// Note that it merely performs a shallow copy of {OutputTable, Pattern} and it copies a few other attributes
	// that are required for correct functionality of Aggregation. Shallow copies are alright as it saves RAM
	// and we do not need (or want) to change anything in these objects.
	// Note also that com.rits.cloning.Cloner was failing on cloning Table.Column.uniqueValueSet with StackOverflow
	// error.
	public Predictor(Predictor other) {
		groupId = other.groupId;                           // Required copy of the int
		sql = other.sql;                                   // Required copy of the String
		baseTarget = other.baseTarget;                     // Required copy of the String
		table = other.table;                               // Just a copy of the pointer is OK
		pattern = other.pattern;                           // Just a copy of the pointer is OK
		timestampDesigned = LocalDateTime.now();           // Newly created
		parameterMap = new TreeMap<>(other.parameterMap);  // Shallow copy of the Map is OK
		columnMap = new TreeMap<>(other.columnMap);        // Shallow copy of the Map is OK
		relevance = new Relevance(other.relevance);        // Deep copy

		// For proper working of the journal
		id = other.id;
		outputTable = other.outputTable;                   // Required copy of the String
        name = other.name;
        longName = other.longName;
        timestampBuilt = other.timestampBuilt;
        timestampDelivered = other.timestampDelivered;
        isOk = other.isOk;
        rowCount = other.rowCount;
        nullCount = other.nullCount;
        isInferiorDuplicate = other.isInferiorDuplicate;
        duplicateName = other.duplicateName;
        candidateState = other.candidateState;
        dataTypeCategory = other.dataTypeCategory;
        dataType = other.dataType;
        dataTypeName = other.dataTypeName;
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
		int length = (identifierLengthMax - 9)/3;

		// Add table name
		String name = trimTo(getOriginalTable(), length);

		// Add column names
		for (String columnName : getColumnMap().values()) {
			name = name + "_" + columnName;
		}

		name = trimTo(name, 2*length+1);

		// Replace special characters with spaces
		String patternName = pattern.name.replaceAll("[^a-zA-Z0-9_\\s]", " ");

		// Convert the string to camelCase and remove the spaces
		patternName = WordUtils.capitalizeFully(patternName, new char[]{' '}).replaceAll(" ", "");

		// We would like to have camelCase, not CamelCase
		patternName = Character.toLowerCase(patternName.charAt(0)) + patternName.substring(1);

		name = name + "_" + patternName;

		// Add parameters. Ignore special symbols (particularly "@" from "@column" parameters)
		// But preserve unicode letters, numbers, underscores.
		for (String parameter : parameterMap.keySet()) {
			name = name + "_" + parameterMap.get(parameter).replaceAll(" ", "").replaceAll("[^\\p{L}0-9_]", "");
		}

		name = trimTo(name, 3*length+2);

		// Add id with zero padding from left
		name = name + "_" + id;

		return name;
	}

	// Account for non-ascii characters taking possibly more than 1 byte.
	// This is important for PostgreSQL, which has a limit in bytes (not characters).
	// If the whole char does not fit completely into the limit it returns a shorter sequence rather
	// than returning a string with a broken character at the end.
	// We assume UTF-8 encoding, as it is a reasonably safe bet. Still, UTF-16 can produce a longer array.
	static protected String trimTo(String name, int byteLength) {
		int i = 0;
		try {
			while ((i+1) <= name.length() && name.substring(0, i+1).getBytes("UTF-8").length <= byteLength) {
				i++;
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return name.substring(0, i);
	}



	////////////// Comparators ////////////////

	// Predictors are sorted by their id in collections like SortedSet
	@Override
	public int compareTo(Predictor anotherPredictor) {
		return id - anotherPredictor.getId();
	}

	// Sort by {isCandidate desc, chosenRelevance desc, runtime asc, id asc}.
	// Justification: we prefer predictors that are ok, are highly relevant and fast to calculate.
	// As a tie breaker, we return predictors that were calculated early (have low id).
	public static final Comparator<Predictor> SingleRelevanceComparator = (o1, o2) -> {
		// Candidate states (predictors that are not OK or are duplicate have candidateState<1)
		if (o1.candidateState > o2.candidateState) {
			return -1;
		} else if (o1.candidateState < o2.candidateState) {
			return 1;
		}

		// Compare based on the chosen relevance
		if (o1.getChosenWeightedRelevance() > o2.getChosenWeightedRelevance()) {
			return -1;
		} else if (o1.getChosenWeightedRelevance() < o2.getChosenWeightedRelevance()) {
			return 1;
		}

		// Compare based on the runtime
		if (o1.getRuntime() < o2.getRuntime()) {
			return -1;
		} else if (o1.getRuntime() > o2.getRuntime()) {
			return 1;
		}

		// Compare based on the id, if necessary
		if (o1.id < o2.id) {
			return -1;
		} else if (o1.id > o2.id) {
			return 1;
		}
		return 0;
	};


	/////////// Equal, HashCode ///////////
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Predictor predictor = (Predictor) o;
		return id == predictor.id;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	/////////// To string ///////////
	@Override
	public String toString() {
		return "Predictor{" +
				"id=" + id +
				", name='" + name + '\'' +
				", candidateState='" + candidateState + '\'' +
				relevance +
				'}';
	}


	/////////// Convenience getters and setters //////////
	public void setParameter(String key, String value) {
		parameterMap.put(key, value);
	}

	public double getWeightedRelevance(String baseTarget) {
		return relevance.getWeightedRelevance(baseTarget);
	}

	public double getRelevance(String baseTarget) {
		return relevance.getRelevance(baseTarget);
	}

	public void setRelevance(String baseTarget, Double value) {
		relevance.setRelevance(baseTarget, value);
	}

	public double getConceptDrift(String baseTarget) {
		return relevance.getConceptDrift(baseTarget);
	}

	public void setConceptDrift(String baseTarget, Double value) {
		relevance.setConceptDrift(baseTarget, value);
	}

	public double getChosenWeightedRelevance() {
		return relevance.getWeightedRelevance(chosenBaseTarget);
	}

	public double getRuntime() {
		// Runtime in seconds with three decimal values. Assumes that start time and end time are available.
		return timestampBuilt.until(timestampDelivered, ChronoUnit.MILLIS) / 1000.0;
	}

	public String getExceptionMessage() {
		if (exception!=null) return exception.getMessage();
		return null;
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

	public List<OptimizeParameters> getPatternOptimizeParameter() {
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

	public String getDataTypeCategory() {
		return dataTypeCategory;
	}

	public void setDataTypeCategory(String dataTypeCategory) {
		this.dataTypeCategory = dataTypeCategory;
	}

	public Pattern getPattern() {
		return pattern;
	}

	public void setException(Exception exception) {
		this.exception = exception;
	}

	public Relevance getRelevanceObject() {
		return relevance;
	}

	public void setRelevanceObject(Relevance relevance) {
		this.relevance = relevance;
	}

	public String getBaseTarget() {
		return baseTarget;
	}

	public void setBaseTarget(String baseTarget) {
		this.baseTarget = baseTarget;
	}

	public String getTargetColumn() {
		return targetColumn;
	}

	public void setTargetColumn(String targetColumn) {
		this.targetColumn = targetColumn;
	}

	public String getChosenBaseTarget() {
		return chosenBaseTarget;
	}

	public void setChosenBaseTarget(String chosenBaseTarget) {
		this.chosenBaseTarget = chosenBaseTarget;
	}

}