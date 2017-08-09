package connection;

import org.apache.commons.lang3.StringUtils;
import parser.ANTLR;
import run.Setting;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

	// Transform SELECT...FROM... into SELECT...INTO...FROM...
	// This transformation is convenient only for MS SQL SERVER/ACCESS
	public static String addIntoClause(String sql) {
		// Initialization
		Pattern pattern = Pattern.compile("(?i)[^@\\S*]from"); // case insensitive, not with @ at the beginning
		Matcher matcher = pattern.matcher(sql);
		int pos = 0;
		boolean flagFound = false;
		int firstPos = -1;

		// Check candidates one by one until you find an acceptable candidate
		while (matcher.find()) {
			pos = matcher.start();
			if (firstPos == -1) {
				firstPos = pos;
			}
			if (isFromClause(sql.substring(0, pos))) {
				flagFound = true;
				break; // We found the FROM term starting at position "pos".
			}
		}

		// If undecided, pick the first found select
		// UGLY HACK FOR DEALING WITH CONFUSING: (...) UNION ALL (...)
		if (!flagFound) {
			pos = firstPos;
		}

		// Return the result
		String substring = sql.substring(0, pos);
		substring = substring + " INTO @outputTable";
		sql = substring + sql.substring(pos);

		return sql;
	}

	// 1) Our FROM term is one of the outermost FROMs in the statement. Just count the opening and closing brackets in
	// the substring preceding the FROM term. If the count is zero, it's one of the outermost FROM terms.
	// 2) Ignore FROM terms in quotes as they are just a part of column/table/schema/database name like "date_from".
	// Simply test that the count of quotes is even.
	// 3) Ignore when it
	// Note: beware of comments in the statement. If there is a comment, the detection doesn't work
	// Note: beware of unnecessary brackets (like wrapping whole SQL in brackets)
	private static boolean isFromClause(String sql) {
		int count;

		count = StringUtils.countMatches(sql, "(") - StringUtils.countMatches(sql, ")");
		if (count != 0) return false;

		count = StringUtils.countMatches(sql, "[") - StringUtils.countMatches(sql, "]");
		if (count != 0) return false;

		count = StringUtils.countMatches(sql, "{") - StringUtils.countMatches(sql, "}");
		if (count != 0) return false;

		count = StringUtils.countMatches(sql, "\"") % 2;
		if (count == 1) return false;

		count = StringUtils.countMatches(sql, "`") % 2;
		if (count == 1) return false;

		count = StringUtils.countMatches(sql, "'") % 2;
		return count != 1;

	}

	// Limit the count of returned rows
	public static String limitResultSet(Setting setting, String sql, int rowCount) {

		// Set top (MSSQL)
		if ("top".equals(setting.limitSyntax)) {
			sql = sql.replaceFirst("(?i)SELECT", "SELECT TOP " + rowCount); // Case insensitive
		}

		// Set limit (PostgreSQL)
		if ("limit".equals(setting.limitSyntax)) {
			sql = sql + " LIMIT " + rowCount + " "; // We have to add the space at the end because of "union all"
		}

		// Set obs (SAS)
		// Applies the condition on the first word immediately after "FROM".
		// Literature: http://support.sas.com/documentation/cdl/en/sqlproc/62086/HTML/default/viewer.htm#a003278683.htm
		// Warning: it's a limit on the input, not on the output.
		// Warning: works only with tables. Fails on views.
		// Also, SAS "eats" line breaks without replacing them with a space -> accidental concats may render the
		// query invalid.
		// Solution: We apply the limit at the JDBC level (of course, it applies only on the result set...)
		if ("obs".equals(setting.limitSyntax)) {
//            Pattern pattern = Pattern.compile("(?i)\\b(.*FROM\\s+\\S+)(.*)");
//            Matcher matcher = pattern.matcher(sql);
//
//            if (matcher.find()) {
//                sql =  matcher.group(1) + "(obs=" + rowCount + ")" + matcher.group(2);
//            }
		}

		// Set rownum (Oracle)
		if ("rownum".equals(setting.limitSyntax)) {
			Pattern pattern = Pattern.compile("(?i)(.*)(WHERE)(.*)"); // Case insensitive + greedy
			Matcher matcher = pattern.matcher(sql);

			if (matcher.find()) {
				// Where condition is already present
				sql = matcher.group(1) + "WHERE ROWNUM <= " + rowCount + " AND" + matcher.group(3);
			} else {
				// BLINDLY ADD WHERE CONDITION
				sql = sql + " WHERE ROWNUM <= " + rowCount;
			}
		}

		return sql;
	}

	public static String expandBase(Setting setting, String sql) {
		if (setting.targetDate == null) {
			sql = sql.replaceAll("(\\w+\\.|)(@base\\b)", "$1@baseId, $1@baseTarget");
		} else {
			sql = sql.replaceAll("(\\w+\\.|)(@base\\b)", "$1@baseId, $1@baseDate, $1@baseTarget");
		}

		return sql;
	}

	public static String expandBasePartitionBy(Setting setting, String sql) {
		if (setting.targetDate == null) {
			sql = sql.replaceAll("(\\w+\\.|)(@basePartitionBy\\b)", "$1@baseId");
		} else {
			sql = sql.replaceAll("(\\w+\\.|)(@basePartitionBy\\b)", "$1@baseId, $1@baseDate");
		}

		return sql;
	}

	// Expand to lists while duplicating the prefix:
	//  "t1".@baseId -> "t1"."id1", "t1"."id2"
	public static String expandToList(Setting setting, String sql, String keyword, List<String> tokens) {
		String result = "";
		Matcher m = Pattern.compile("(\\w+\\.|)(" + keyword + "\\b)(\\w*)").matcher(sql);
		int start = 0;
		while (m.find()) {
			result += sql.substring(start, m.start()) + escapePrefixedList(setting, m.group(1), tokens);
			start = m.end();
		}
		result += sql.substring(start);

		return result;
	}

	// Escape each item in the list and add the provided prefix in front of each item.
	private static String escapePrefixedList(Setting setting, String prefix, List<String> tokens) {
		String result = "";
		for (String token : tokens) {
			result = result + ", " + prefix + setting.quoteEntityOpen + token + setting.quoteEntityClose;
		}
		result = result.substring(2); // Remove the first two symbols

		return result;
	}

	// Replace:
	//      SELECT EXISTS (...)
	// with:
	//      SELECT COUNT(*)>1 FROM (...)
	//
	// SAS:   SAS does not support "generation" of data from subqueries.
	//        SAS supports only WHERE EXISTS and HAVING EXISTS.
	//        Reference: http://support.sas.com/documentation/cdl/en/sqlproc/63043/HTML/default/viewer.htm#p1st65qbmqdks3n1mch4yfcctexi.htm
	// MSSQL: MSSQL does not support SELECT EXISTS.
	//        MSSQL allows EXISTS clause only in CASE WHEN EXISTS or WHERE EXISTS.
	//        Reference: http://stackoverflow.com/questions/2759756/is-it-possible-to-select-exists-directly-as-a-bit
	// ORACLE: Oracle supports only WHERE EXISTS, because Oracle does not known a concept of a boolean.
	//        Reference: http://stackoverflow.com/questions/3726758/is-there-a-boolean-type-in-oracle-databases
	// PostgreSQL and MariaDB are ok.
	public static String replaceExists(Setting setting, String sql) {
		if (setting.supportsSelectExists) return sql;

		Pattern pattern = Pattern.compile("(?i)SELECT\\s*EXISTS([\\S\\s]*)");
		Matcher matcher = pattern.matcher(sql);

		if ("Microsoft SQL Server".equals(setting.databaseVendor)) {
			if (matcher.find()) {
				sql = "SELECT CASE WHEN (EXISTS " + matcher.group(1) + ") THEN 1 ELSE 0 END";
			}
			return sql.trim();
		}

		// SAS and the rest
		if (matcher.find()) {
			sql = "SELECT COUNT(*)>0 FROM " + matcher.group(1); // MonetDB requires named subselect, but that may result in the table name clash
		}

		return sql.trim();
	}

	public static String escape(Setting setting, String sql) {
		sql = sql.replace("@targetValue", setting.quoteAliasOpen + "@targetValue" + setting.quoteAliasClose);

		return sql;
	}

	// Returns vendor's dialect SQL.
	public static String getDialectCode(Setting setting, String agnosticCode) {
		// Expand dateDiff, dateToNumber, corr...
		String dialectCode = ANTLR.parseSQL(setting, agnosticCode);

		// Stddev_samp
		dialectCode = dialectCode.replaceAll("(?i)stdDev_samp", setting.stdDevSampCommand);

		// Stddev_pop
		dialectCode = dialectCode.replaceAll("(?i)stdDev_pop", setting.stdDevPopCommand);

		// charLengthCommand
		dialectCode = dialectCode.replaceAll("(?i)char_length", setting.charLengthCommand);

		// Timestamp
		dialectCode = dialectCode.replaceAll("(?i)timestamp", setting.typeTimestamp);



		// NullIf (SAS doesn't support nullIf command in SQL over JDBC, rewrite nullIf with basic commands)
		if ("SAS".equals(setting.databaseVendor)) {
			dialectCode = nullIf(dialectCode);
		}

		return dialectCode;
	}

	// Subroutine for getDialectCode
	private static String nullIf(String dialectCode) {
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(?is)(.*)(nullif\\()(.*?)(,+)(.*?)(\\))(.*)");

		do {
			Matcher matcher = pattern.matcher(dialectCode);
			if (matcher.find()) {
				// Extract the keywords
				String what = matcher.group(3);
				String value = matcher.group(5);

				// Replace
				String caseWhen = "CASE WHEN @what = @value THEN null ELSE @what END";
				caseWhen = caseWhen.replace("@what", what);
				caseWhen = caseWhen.replace("@value", value);
				dialectCode = matcher.group(1) + caseWhen + matcher.group(7);
			} else {
				break;
			}
		} while (true);

		return dialectCode;
	}

}
