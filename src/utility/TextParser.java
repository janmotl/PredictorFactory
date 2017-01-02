package utility;

import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;


public class TextParser {

	// Logging
	private static final Logger logger = Logger.getLogger(TextParser.class.getName());

	// Convert comma delimited string to list
	@NotNull public static List<String> string2list(@Nullable String string) {
		// Initialization
		List<String> result = new ArrayList<>();

		// Deal with nulls and empty strings
		if (string == null || string.isEmpty()) return result;

		// Parsing
		result = Arrays.asList(string.split(","));

		// Strip leading and trailing spaces
		result.replaceAll(x -> x.trim());

		return result;
	}

	// Convert dot delimited list to map
	// Example: table.column -> key=table, value=column
	@NotNull public static Map<String, List<String>> list2map(@NotNull List<String> list) {
		Map<String, List<String>> result = new HashMap<>();

		for (String tuple : list) {
			String[] tableColumn = tuple.split("\\."); // Dot, but escaped for regex
			if (tableColumn.length != 2) {  // Take care of empty strings...
				logger.warn("Expected string in the form table.column but obtained: " + tuple);
				continue;
			}
			String table = tableColumn[0];
			String column = tableColumn[1];

			if (result.containsKey(table)) {
				result.get(table).add(column); // Add column into a present table
			} else {
				result.put(table, new LinkedList<>(Arrays.asList(column))); // Make a new table with the column
			}
		}

		return result;
	}
}
