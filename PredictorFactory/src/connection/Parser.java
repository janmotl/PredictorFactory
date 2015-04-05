package connection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class Parser {

	public static String addIntoClause(String sql) {
		// Initialization
	    Pattern pattern = Pattern.compile("(?i)[^@\\S*]from"); // case insensitive, not with @ at the beginning
	    Matcher matcher = pattern.matcher(sql);
	    int pos = 0;
	    
	    // Check candidates one by one until you find an acceptable candidate
	    while (matcher.find()) {
	    	pos = matcher.start();
	    	if (isFromClause(sql.substring(0, pos))) break; // We found the FROM term starting at position "pos".
	        System.out.print("Start index: " + matcher.start());
	        System.out.print(" End index: " + matcher.end());
	        System.out.println(" Found: " + matcher.group());
	    }
		
	    // Return the result
	    String substriString = sql.substring(0, pos);
	    substriString = substriString + " INTO @outputTable";
	    sql = substriString + sql.substring(pos);
	    
	    return sql;
	}
	
	// 1) Our FROM term is one of the outermost FROMs in the statement. Just count the opening and closing brackets in 
	// the substring preceding the FROM term. If the count is zero, it's one of the outermost FROM terms.
	// 2) Ignore FROM terms in quotes as they are just a part of column/table/schema/database name like "date_from".
	// Simply test that the count of quotes is even.
	// 3) Ignore when it 
	// Note: beware of comments in the statement. If there is a comment, the detection doesn't work
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
		if (count == 1) return false;

		return true;
	}
}
