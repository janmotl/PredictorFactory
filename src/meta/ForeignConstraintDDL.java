package meta;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ForeignConstraintDDL {
	// Logging
	private static final Logger logger = Logger.getLogger(ForeignConstraintDDL.class.getName());

	public static List<ForeignConstraint> unmarshall(String fileName) {
		List<ForeignConstraint> result = new ArrayList<>();
		Path path = Paths.get("config", fileName);

		if (Files.exists(path)) {   // Ignore absence of the file
			try {
				byte[] encoded = Files.readAllBytes(path);
				String sql = new String(encoded, StandardCharsets.UTF_8);
				result = extract(sql);
			} catch (IOException e) {
				logger.warn("Attempt to parse 'config/" + fileName + "' failed.");
			}
		}

		return result;
	}

	public static List<ForeignConstraint> extract(String sql) {
        List<ForeignConstraint> result = new ArrayList<>();

		// Prepare regex matcher
		// Note: The regex is very simple and parses EVEN comments, requires termination semicolon...
		String regex = " ALTER TABLE id ADD (?: | CONSTRAINT id ) FOREIGN KEY \\(id\\) REFERENCES id \\(id\\) anything ; ";
		regex = regex.replace(" ", "\\s*");             // Whitespace characters
		regex = regex.replace("id", "([^;]+?)");        // ID group (any character(s) but semicolon, not greedy)
		regex = regex.replace("anything", "[^;]*?");    // Like "NOT DEFERRABLE"...
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

		Matcher matcher = pattern.matcher(sql);
		while (matcher.find()) {
			// Note: Will fail if a comma is used in a name (we should respect the quotation marks)
			String fTable = "";
			String fSchema = "";
			String[] foreign = matcher.group(1).trim().split("\\."); // Split on dot, if possible
			if (foreign.length==0) {
				fTable = matcher.group(1).trim();
			} else if (foreign.length==1) {
				fTable = foreign[0];
			} else if (foreign.length>=2) {
				fTable = foreign[foreign.length-1]; // The last item
				fSchema = foreign[foreign.length-2]; // The second from the end
			}

			// Name is optional
			String name = null;
			if (matcher.group(2) != null) {
				name = matcher.group(2).trim();
			}

			// Note: Comma in a quoted name will cause troubles...
			List<String> fColumns = Arrays.stream(matcher.group(3).split(",")).map(String::trim).map(ForeignConstraintDDL::trimQuotes).collect(Collectors.toList());

			// Note: Will fail if a comma is used in a name (we should respect the quotation marks)
			String table = "";
			String schema = "";
			String[] local = matcher.group(4).trim().split("\\.");
			if (local.length==0) {
				table = matcher.group(4).trim();
			} else if (local.length==1) {
				table = local[0];
			} else if (local.length>=2) {
				table = local[local.length-1]; // The last item
				schema = local[local.length-2]; // The second from the end
			}

			// Note: Comma in a quoted name will cause troubles...
			List<String> columns = Arrays.stream(matcher.group(5).split(",")).map(String::trim).map(ForeignConstraintDDL::trimQuotes).collect(Collectors.toList());

			ForeignConstraint fc = new ForeignConstraint(name, fSchema, fTable, schema, table, fColumns, columns);
			result.add(fc);
		}

		return result;
	}

	public static String trimQuotes(String input) {
		return input.replaceAll("^(\"|`)|(\"|`)$", ""); // Remove the starting and ending quotes
	}

	// More specific getter. Return all FC related to the table.
	// But if there is a self-referencing FC, include that FC just once.
	public static List<ForeignConstraint> getForeignConstraintList(String fileName, String tableName) {

		// Initialisation
		List<ForeignConstraint> result = new ArrayList<>();

		// Select the appropriate foreign constrains
		for (ForeignConstraint foreignConstraint : unmarshall(fileName)) {
			if (foreignConstraint.table.equals(tableName)) {
				result.add(foreignConstraint);
			} else if (foreignConstraint.fTable.equals(tableName)) {
				result.add(foreignConstraint);
			}
		}

		return result;
	}

}