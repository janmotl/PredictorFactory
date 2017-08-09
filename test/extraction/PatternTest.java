package extraction;


import org.junit.Test;
import run.Setting;
import utility.PatternMap;

import java.util.SortedMap;


public class PatternTest {

	private Setting setting = new Setting();

	@Test
	public void allPatterns() {

		// Initialization
		SortedMap<String, Pattern> patternMap = PatternMap.getPatternMap();
		Setting setting = new Setting("Azure", "financial_test_setting");


		// Loop over each pattern and tests that all patterns are parsed without an error (IN CONSOLE!)
		for (Pattern pattern : patternMap.values()) {
			pattern.initialize(setting);
			String code = pattern.dialectCode;
			System.out.println(pattern.name);
			System.out.println(code);
			System.out.println("----");
		}
	}
	

}
