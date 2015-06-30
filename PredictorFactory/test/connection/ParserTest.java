package connection;

import org.junit.Assert;
import org.junit.Test;

import run.Setting;

public class ParserTest {

	
  @Test
  public void testObs() {
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
  public void testObs2() {
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
  
  
}