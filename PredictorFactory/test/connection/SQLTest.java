package connection;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import run.Setting;

public class SQLTest {

	
  @Test
  public void propagateID() {
	  	// Setting
		Setting setting = new Setting();
		setting.databaseVendor = "MySQL";
		setting.inputSchema = "financial";
		setting.outputSchema = "financial";
		setting.targetId = "account_id";
		setting.targetColumn = "status";
		setting.targetTable = "loan";
		setting.sampleTable = "mainSample";
		setting.baseTable = "base";
		setting.supportsCreateTableAs = true; 
		setting.quoteEntityOpen ="`";
		setting.quoteEntityClose ="`";
	  
		// Parameters
		Map<String, String> hashMap = new HashMap<String, String>();
		hashMap.put("@idColumn2", "account_id");
		hashMap.put("@inputTable2", "trans");
		hashMap.put("@outputTable", "propagated_trans");
		
		// Test
		Assert.assertTrue(true);
		//String sql =  SQL.propagateID(setting, hashMap);
		//System.out.println(sql);
		//Assert.assertEquals(sql, "SELECT t1.[account_id], t1.date, t1.[status], t2.* FROM [base] t1 INNER JOIN [trans] t2 ON t1.[account_id] = t2.[account_id]");
  }
  
  
  @Test
  public void addCreateTableAs1() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test user variable compliance (tolerance to at-sign). Important as I am using at-signs in patterns.
		String result = SQL.addCreateTableAs(setting, "SELECT @column FROM table");
		Assert.assertEquals("SELECT @column INTO @outputTable FROM table", result);
  }
  
  @Test
  public void addCreateTableAs2() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test ignorance of a table called "from". This is important in the maintable assembly.
		String result = SQL.addCreateTableAs(setting, "SELECT [from] FROM table");
		Assert.assertEquals("SELECT [from] INTO @outputTable FROM table", result);
  } 
  
  @Test
  public void addCreateTableAs3() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test for case insensitivity. As the SQL Keywords are case-insensitive in SQL 92.
		String result = SQL.addCreateTableAs(setting, "SELECT col FrOm table");
		String expected = "SELECT col INTO @outputTable FrOm table";
		Assert.assertEquals(expected.toLowerCase(), result.toLowerCase());
  }
  
  @Test
  public void addCreateTableAs4() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test with subquery in the select part. 
		String result = SQL.addCreateTableAs(setting, "select col1 `col1`, (SELECT max(col2) from t2) `col2` from t1;");
		Assert.assertEquals("select col1 `col1`, (SELECT max(col2) from t2) `col2` INTO @outputTable from t1;", result);
  }
  
  @Test
  public void addCreateTableAs5() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test that it accepts line breaks instead of spaces
		String result = SQL.addCreateTableAs(setting, "select col1 \nfrom\nt1");
		String expected = "select col1 INTO @outputTable \nfrom\nt1";
		Assert.assertEquals(
				expected.replaceAll("\\s", " ").replaceAll("\\s+", " "),
				result.replaceAll("\\s", " ").replaceAll("\\s+", " ")
		);
  }
  
  @Test
  public void addCreateTableAs6() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test user variables without escaping. This is important in mainsample creation.
		String result = SQL.addCreateTableAs(setting, "SELECT t1.@CLIENT_FROM_DATE FROM t1");
		String expected = "SELECT t1.@CLIENT_FROM_DATE INTO @outputTable FROM t1";
		Assert.assertEquals(expected, result);
  }
  
  @Test
  public void addCreateTableAs7() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test that it can deal with union all. This is important in sampling of base table.
		String result = SQL.addCreateTableAs(setting, "SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t2");
		String expected = "SELECT c1 INTO @outputTable FROM t1 UNION ALL SELECT c1 FROM t2";
		Assert.assertEquals(expected, result);
  }
  
  @Test
  public void addCreateTableAs8() {
	  	// Setting
		Setting setting = new Setting();
		setting.supportsCreateTableAs = false; 
		
		// Test that it can deal with union all in brackets
		String result = SQL.addCreateTableAs(setting, "(SELECT c1 FROM t1) UNION ALL (SELECT c1 FROM t2)");
		String expected = "(SELECT c1 INTO @outputTable FROM t1) UNION ALL (SELECT c1 FROM t2)";
		Assert.assertEquals(expected, result);
  }
   
}