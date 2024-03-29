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

	public static List<ForeignConstraint> unmarshall(String fileName, String defaultSchema) {
		List<ForeignConstraint> result = new ArrayList<>();
		Path path = Paths.get("config", fileName);

		if (Files.exists(path)) {   // Ignore absence of the file
			try {
				byte[] encoded = Files.readAllBytes(path);
				String sql = new String(encoded, StandardCharsets.UTF_8);
				result = extract(sql, defaultSchema);
				logger.info("In total " + result.size() + " foreign key constraints were loaded from the DDL file.");
			} catch (IOException e) {
				logger.warn("Attempt to parse 'config/" + fileName + "' failed.");
			}
		} else {
			logger.debug("File 'config/" + fileName + "' does not exist. Skipping the DDL import.");
		}

		return result;
	}

	// Note: Matching is slow. Particularly repeated spaces seems to slowdown the parsing.
	// See readRecords_mysqlWithSpaces() unit test for the demonstration.
	protected static List<ForeignConstraint> extract(String sql, String defaultSchema) {
		List<ForeignConstraint> result = new ArrayList<>();

		// Prepare regex matcher
		// Note: The regex is very simple and parses comments, requires termination semicolon...
		String regex = "ALTER TABLE id ADD (?:CONSTRAINT id)? FOREIGN KEY \\(id\\) REFERENCES id \\(id\\) anything;";
		regex = regex.replace(" ", "\\s*+");            // Whitespace characters (be greedy and never backtrack -> faster)
		regex = regex.replace("id", "([^;]+?)");        // ID group (any character(s) but semicolon, lazy "non-greedy" evaluation)
		regex = regex.replace("anything", "[^;]*+");    // Like "NOT DEFERRABLE"... (be greedy until you spot a semicolon)
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

		Matcher matcher = pattern.matcher(sql);
		while (matcher.find()) {
			// Note: Will fail if a comma is used in a name (we should respect the quotation marks)
			String fTable = null;
			String fSchema = null;
			String[] foreign = matcher.group(1).trim().split("\\."); // Split on dot, if possible
			if (foreign.length == 0) {
				fTable = matcher.group(1).trim();
			} else if (foreign.length == 1) {
				fTable = foreign[0];
			} else if (foreign.length >= 2) {
				fTable = foreign[foreign.length - 1]; // The last item
				fSchema = foreign[foreign.length - 2]; // The second from the end
			}

			// Name is optional
			String name = null;
			if (matcher.group(2) != null) {
				name = matcher.group(2).trim();
			}

			// Note: Comma in a quoted name will cause troubles...
			List<String> fColumns = Arrays.stream(matcher.group(3).split(",")).map(String::trim).map(ForeignConstraintDDL::trimQuotes).collect(Collectors.toList());

			// Note: Will fail if a comma is used in a name (we should respect the quotation marks)
			String table = null;
			String schema = null;
			String[] local = matcher.group(4).trim().split("\\.");
			if (local.length == 0) {
				table = matcher.group(4).trim();
			} else if (local.length == 1) {
				table = local[0];
			} else if (local.length >= 2) {
				table = local[local.length - 1]; // The last item
				schema = local[local.length - 2]; // The second from the end
			}

			// Note: Comma in a quoted name will cause troubles...
			List<String> columns = Arrays.stream(matcher.group(5).split(",")).map(String::trim).map(ForeignConstraintDDL::trimQuotes).collect(Collectors.toList());

			// Use the default schema, if necessary
			if (schema == null) schema = defaultSchema;
			if (fSchema == null) fSchema = defaultSchema;

			ForeignConstraint fc = new ForeignConstraint(trimQuotes(name), trimQuotes(fSchema), trimQuotes(fTable), trimQuotes(schema), trimQuotes(table), fColumns, columns);
			result.add(fc);
		}

		return result;
	}

	protected static String trimQuotes(String input) {
		if (input == null) return null;
		return input.replaceAll("^(\"|`)|(\"|`)$", ""); // Remove the starting and ending quotes
	}


}
