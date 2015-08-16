package connection;

import org.junit.Assert;
import org.junit.Test;

import run.Setting;

public class ParserTest {

	
  @Test
  public void testObs_end() {
	  	// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "obs";
		String sql = "select col1 from tab1";
		int rowCount = 10;
				
		
		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1(obs=10)";  
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
  }
  
  @Test
  public void testObs_middle() {
	  	// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "obs";
		String sql = "select col1 from tab1 where col2 = 'orchid'";
		int rowCount = 10;
				
		
		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1(obs=10) where col2 = 'orchid'";  
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
  }
  
  @Test
  public void testRowNum_zeroOccurence() {
	  	// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "rownum";
		String sql = "select col1 from t1";
		int rowCount = 10;
				
		
		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from t1 WHERE ROWNUM <= 10";  
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
  }
  
  @Test
  public void testRowNum_oneOccurence() {
	  	// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "rownum";
		String sql = "select col1 from tab1 where col1 <> 'a'";
		int rowCount = 10;
				
		
		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1 WHERE ROWNUM <= 10 AND col1 <> 'a'";  
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
  }
  
  @Test
  public void testRowNum_twoOccurences() {
	  	// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "rownum";
		String sql = "select col1 from (select col1 from t1 where col2 = 'b') t2 where col1 <> 'a'";
		int rowCount = 10;
				
		
		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from (select col1 from t1 where col2 = 'b') t2 WHERE ROWNUM <= 10 AND col1 <> 'a'";  
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
  }
  
}