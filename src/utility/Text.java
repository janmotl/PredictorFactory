package utility;

import org.jetbrains.annotations.NotNull;

import java.util.*;


public class Text {
    // Convert comma delimited string to list
    @NotNull
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
    @NotNull
    public static Map<String, List<String>> list2map(@NotNull List<String> list) {
        Map<String, List<String>> result = new HashMap<>();

        for (String tuple : list) {
            String[] tableColumn = tuple.split("\\."); // Dot, but escaped for regex
            if (tableColumn.length != 2) continue;  // Take care of empty strings...
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
