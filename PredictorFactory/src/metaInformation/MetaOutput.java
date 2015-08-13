package metaInformation;

import utility.Meta;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

// Table metadata
public class MetaOutput {
	
	// Define struct
	public static class OutputTable extends Meta.Table{
		public String originalName;				// The table name before propagation.
		public String propagatedName;			// The table name after propagation.
		public String propagationDate;			// Column used for time constrain during table propagation (in the last table).
		public int propagationOrder;			// The order, in which tables are propagated.
		public List<String> propagationPath;	// In the case of loops this makes the difference.
		public String sql;						// SQL code used for creation of the propagated table.
	}
	
	// Export SQL code for target table propagation.
	public static void exportPropagationSQL(SortedMap<String, OutputTable> metaOutput) {
		try (PrintWriter writer = new PrintWriter("./log/propagation.sql", "UTF-8")) {

			// Export the code in order of execution (by propagationOrder)
			SortedMap<Integer, String> map = new TreeMap<Integer, String>();

			for (OutputTable outputTable : metaOutput.values()) {
				map.put(outputTable.propagationOrder, outputTable.sql);
			}

			for (String sql : map.values()) {
				writer.println(sql + ";");
			}

		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
}
