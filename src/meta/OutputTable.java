package meta;

import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlType;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

// Table metadata
@XmlType(name = "outputTable")
public class OutputTable extends Table {
	public String originalName;                     // The table name before propagation.
	public String temporalConstraint;               // Column used for time constraint during table propagation (in the last table).
	public String temporalConstraintJustification;  // Textual justification behind the selection of temporalConstraint.
	public Integer temporalConstraintRowCountOptimistic; // An optimistic estimate of propagated rows.
	public String sql;                      // SQL code used for creation of the propagated.
	public String propagationTable;         // This is the name of the outputTable, from which we collect {targetId,...}.

	@XmlIDREF public List<OutputTable> propagationTables = new ArrayList<>(); // All outputTables (including FKCs!) over which the table was propagated. Since it contains only links, it is small in RAM. However, to prevent the bloat of the XML, we have to use {@XmlIDREF @XmlID} trick.
	public List<String> propagationPath = new ArrayList<>();    // In the case of loops this makes the difference. It uses input table names -> useful only for reporting. Can be completely replaced with propagationTables.
	public int propagationOrder;            // The order, in which tables are propagated.
	public Integer rowCount;                // Count of tuples in the calculated feature.
	public boolean dateBottomBounded;       // Related to time window NOT PROPERLY USED!
	public boolean isIdUnique;              // Is the relation target_id:this_id in 1:1 or 1:n?
	public boolean isTargetIdUnique;        // Does combination {baseId, baseDate} repeat in this table? NOTE: What is the difference between this and isIdUnique?
	public boolean isSuccessfullyExecuted;  // As reported by the database.
	public boolean isOk;                    // Multiple test (isSuccessfullyExecuted && rowCount > 0).
	public final LocalDateTime timestampDesigned;
	public LocalDateTime timestampDelivered;
	public ForeignConstraint propagationForeignConstraint;  // This is the used FC from propagationTable.


	// Constructor
	public OutputTable() {
		timestampDesigned = LocalDateTime.now();
	}

	// Constructor
	public OutputTable(Table table) {
		timestampDesigned = LocalDateTime.now();

		columnMap = table.columnMap;                            // Inherited
		foreignConstraintList = table.foreignConstraintList;    // Inherited
		originalName = table.name;                              // Inherited
		schemaName = table.schemaName;                          // Inherited
	}

}

