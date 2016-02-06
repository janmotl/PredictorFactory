package connection;

import org.apache.commons.lang3.StringUtils;
import run.Setting;

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
		
		count = StringUtils.countMatches(sql, "\"")%2;
		if (count == 1) return false;
		
		count = StringUtils.countMatches(sql, "`")%2;
		if (count == 1) return false;
		
		count = StringUtils.countMatches(sql, "'")%2;
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
			sql = sql + " LIMIT " + rowCount + " ";	// We have to add the space at the end because of "union all"
		}
		
		// Set obs (SAS)
		// Applies the condition on the first word immediately after "FROM"
		// Warning: it's a limit on the input, not on the output
		if ("obs".equals(setting.limitSyntax)) {
			Pattern pattern = Pattern.compile("(?i)\\b(.*FROM\\s+\\S+)(.*)");
			Matcher matcher = pattern.matcher(sql);
			
			if (matcher.find()) {
			    sql =  matcher.group(1) + "(obs=" + rowCount + ")" + matcher.group(2);
			}
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
}
