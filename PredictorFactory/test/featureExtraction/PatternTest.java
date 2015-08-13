package featureExtraction;


import org.junit.Assert;
import org.junit.Test;

import run.Setting;
import utility.PatternMap;

import java.util.SortedMap;


public class PatternTest {

	private Pattern pattern = new Pattern();
	private Setting setting = new Setting();

	@Test
	public void corr() {
		setting.corrSyntax = "corr(@column1, @column2)";
		String code = "SELECT corr(colA,colB) FROM tab1";
		String expected = "SELECT corr(colA, colB) FROM tab1";
		String actual = pattern.getDialect(setting, code);

		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void corrOfCorr() {
		setting.corrSyntax = "corr(@column1, @column2)";
		String code = "SELECT corr(colA,corr(colB,colC)) FROM tab1";
		String expected = "SELECT corr(colA, corr(colB,colC)) FROM tab1";
		String actual = pattern.getDialect(setting, code);

		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void corrMySQL() {
		setting.corrSyntax = "((Avg(@column1 * @column2) - Avg(@column1) * Avg(@column2)) / (stdDev_samp(@column1) * stdDev_samp(@column2)))";
		String code = "SELECT corr(col1,col2) FROM tab1";
		String expected = "SELECT ((Avg(col1 * col2) - Avg(col1) * Avg(col2)) / (stdDev_samp(col1) * stdDev_samp(col2))) FROM tab1";
		String actual = pattern.getDialect(setting, code);

		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}


	@Test
	public void timeDiffMultiple() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT col1 FROM tab1 WHERE dateDiff(col2,col3) > 10 AND dateDiff(col4,col5) < 5";
		String expected = "SELECT col1 FROM tab1 WHERE (col2 - col3) > 10 AND (col4 - col5) < 5";
		String actual = pattern.getDialect(setting, code);
				
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void timeDiffLineWrap() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT col1 FROM tab1 WHERE dateDiff(col2,\ncol3) > 10";
		String expected = "SELECT col1 FROM tab1 WHERE (col2 - \ncol3) > 10";
		String actual = pattern.getDialect(setting, code);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void timeDiffFunction() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT @baseId, DATEDIFF(@baseDate,max(@timeColumn, @baseId))";
		String expected = "SELECT @baseId, (@baseDate - max(@timeColumn, @baseId))";
		String actual = pattern.getDialect(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void timeDiffInFunction() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT @baseId, @aggregateFunction(DATEDIFF(@baseDate,@timeColumn))";
		String expected = "SELECT @baseId, @aggregateFunction((@baseDate - @timeColumn))";
		String actual = pattern.getDialect(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void timeDiffMySQL() {
		setting.dateDiffSyntax = "DATEDIFF(@dateTo, @dateFrom)";
		String code = "DATEDIFF(@baseDate,@timeColumn)";
		String expected = "DATEDIFF(@baseDate, @timeColumn)";
		String actual = pattern.getDialect(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void allPatterns() {

		// Initialization
		SortedMap<String, Pattern> patternMap = PatternMap.getPatternMap();
		Setting setting = new Setting("Azure", "financial_test_setting");


		// Loop over each pattern and tests that all patterns are parsed without an error (IN CONSOLE!)
		for (Pattern pattern : patternMap.values()) {
			pattern.agnostic2dialectCode(setting);
			String code = pattern.dialectCode;
			System.out.println(pattern.name);
			System.out.println(code);
			System.out.println("----");
		}
	}
	

}
