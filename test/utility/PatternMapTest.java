package utility;


import featureExtraction.Pattern;
import run.Setting;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Matcher;

public class PatternMapTest {

	//@Test
	public static void main(String[] arg ) {
    	SortedMap<String, Pattern> map = PatternMap.getPatternMap();
		Setting setting = new Setting("PostgreSQL", "financial");
		String regexPattern = "@\\w+Column\\w*";

		// Header
		System.out.println("pattern \t cardinality \t numerical \t nominal \t date \t polyvariate \t base_target \t base_date");

		// Individual rows
		for (Pattern pattern : map.values()) {
			pattern.initialize(setting);

			// Get unique columns
			Matcher m = java.util.regex.Pattern.compile(regexPattern).matcher(pattern.dialectCode);
			Set set = new HashSet<>();
			while (m.find()) {
				set.add(m.group());
			}

			// Get columns from the parameter
			boolean numerical = false;
			boolean nominal = false;
			boolean time = false;
			int columnCount = 0;
			for (String parameter : pattern.dialectParameter.values()) {
				numerical = parameter.contains("@numericalColumn");
				nominal = parameter.contains("@nominalColumn");
				time = parameter.contains("@timeColumn");
				columnCount =+  ((parameter.contains("@numericalColumn") || parameter.contains("@nominalColumn") || parameter.contains("@timeColumn")) ? 1:0);
			}

			System.out.println(pattern.name
					+ "\t" + pattern.cardinality
					+ "\t" + (pattern.dialectCode.contains("@numericalColumn") || numerical)
					+ "\t" + (pattern.dialectCode.contains("@nominalColumn")  || nominal)
					+ "\t" + (pattern.dialectCode.contains("@timeColumn")  || time)
					+ "\t" + (set.size() + columnCount)
					+ "\t" + pattern.dialectCode.contains("@targetValue")
					+ "\t" + pattern.requiresBaseDate);
		}
	}

}
