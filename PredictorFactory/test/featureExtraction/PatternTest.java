package featureExtraction;


import junit.framework.Assert;

import org.junit.Test;

import run.Setting;


public class PatternTest {

	private Pattern pattern = new Pattern();
	private Setting setting = new Setting();
		
	@Test
	public void timeDiffMultiple() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT col1 FROM tab1 WHERE dateDiff(col2, col3) > 10 AND dateDiff(col2, col3) < 5";
		String observed = pattern.getDialect(setting, code);
		String expected = "SELECT col1 FROM tab1 WHERE (col2 -  col3) > 10 AND (col2 -  col3) < 5";
				
		Assert.assertEquals(expected, observed);
	}
	
	@Test
	public void timeDiffLineWrap() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT col1 FROM tab1 WHERE dateDiff(col2, \ncol3) > 10";
		String observed = pattern.getDialect(setting, code);
		String expected = "SELECT col1 FROM tab1 WHERE (col2 -  \ncol3) > 10";
				
		System.out.println(observed);
		Assert.assertEquals(expected, observed);
	}
	

}
