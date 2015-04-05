package propagation;

import java.util.List;

import utility.Meta;

// Table metadata
public class MetaOutput {
	
	// Define struct
	public static class OutputTable extends Meta.Table{
		public String originalName;				// The table name before propagation
		public String propagatedName;			// The table name after propagation
		public String propagationDate;			// Column used for time constrain during table propagation (in the last table).
		public List<String> propagationPath;	// In the case of loops this makes the difference.
	}	
}
