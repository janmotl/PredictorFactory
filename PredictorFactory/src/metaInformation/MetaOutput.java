package metaInformation;

import utility.Meta;

import java.time.LocalDateTime;
import java.util.List;

// Table metadata
public class MetaOutput {
	
	// Define struct
	public static class OutputTable extends Meta.Table{
		public String originalName;				// The table name before propagation.
		public String propagatedName;			// The table name after propagation.
		public String constrainDate;			// Column used for time constrain during table propagation (in the last table).
		public int propagationOrder;			// The order, in which tables are propagated.
		public List<String> propagationPath;	// In the case of loops this makes the difference.
		public Integer rowCount;				// Count of tuples in the table. Permit null.
		public boolean isIdUnique;				// Is the relation target_id:this_id in 1:1 or 1:n?
		public String sql;						// SQL code used for creation of the propagated table.
		public boolean isSuccessfullyExecuted;
		public final LocalDateTime timestampDesigned;
		public LocalDateTime timestampDelivered;
		public ForeignConstraint foreignConstraint;
		public boolean dateBottomBounded;
		public String propagationTable; 		// The table, from which propagated_id... are obtained

		// Constructor
		public OutputTable() {
			timestampDesigned = LocalDateTime.now();
		}
	}
}
