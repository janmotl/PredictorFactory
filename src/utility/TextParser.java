package utility;

import org.apache.log4j.Logger;

import java.util.*;

// Note: We really ought to refactor XML schema and use a true hierarchical structure in the XML.
public class TextParser {

	// Logging
	private static final Logger logger = Logger.getLogger(TextParser.class.getName());

	// Convert comma delimited string to list
	// Note: Hope that no one is using commas in the names...
	// Solution: Refactor XML to not use attributes but fields. With that we will be able to properly model lists.
	public static List<String> string2list(String string) {
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
	// Note: Hope that no one gets the brilliant idea to use dots in the names...
	public static Map<String, List<String>> list2map(List<String> list, String defaultSchema) {
		Map<String, List<String>> result = new HashMap<>();
		String schema;
		String table;

		for (String tuple : list) {
			String[] tableColumn = tuple.split("\\."); // Dot, but escaped for regex
			if (tableColumn.length == 2) {
				schema = tableColumn[0];
				table = tableColumn[1];
			} else if (tableColumn.length == 1) {
				schema = defaultSchema;
				table = tableColumn[0];
				logger.trace("Expected string in the form 'schema.table' but obtained: '" + tuple + "'. Target schema name was used as the prefix.");
			} else {
				logger.warn("Expected string in the form 'schema.table' but obtained: " + tuple + "'.");
				continue;   // Skip the record
			}

			if (result.containsKey(schema)) {
				result.get(schema).add(table); // Add table into a present schema
			} else {
				result.put(schema, new LinkedList<>(Arrays.asList(table))); // Make a new schema with the table
			}
		}

		return result;
	}

	public static Map<String, Map<String, List<String>>> list2mapMap(List<String> list, String defaultSchema) {
		Map<String, Map<String, List<String>>> result = new HashMap<>();
		String schema;
		String table;
		String column;

		for (String tuple : list) {
			String[] schemaTableColumn = tuple.split("\\."); // Dot, but escaped for regex
			if (schemaTableColumn.length == 3) {
				schema = schemaTableColumn[0];
				table = schemaTableColumn[1];
				column = schemaTableColumn[2];
			} else if (schemaTableColumn.length == 2) {
				schema = defaultSchema;
				table = schemaTableColumn[0];
				column = schemaTableColumn[1];
				logger.trace("Expected string in the form 'schema.table.column' but obtained: '" + tuple + "'. Target schema name was used as the prefix.");
			} else {
				logger.warn("Expected string in the form 'schema.table.column' but obtained: '" + tuple + "'.");
				continue;   // Skip the record
			}

			if (!result.containsKey(schema)) {
				result.put(schema, new HashMap<>()); // Create a new schema
			}

			addTable(result, schema, table, column);
		}

		return result;
	}

	// Subroutine
	private static void addTable(Map<String, Map<String, List<String>>> result, String schema, String table, String column) {
		if (result.get(schema).containsKey(table)) {
			result.get(schema).get(table).add(column); // Add column into a present table
		} else {
			result.get(schema).put(table, new LinkedList<>(Arrays.asList(column))); // Make a new table with the column
		}
	}
}
