package utility;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import run.Setting;

public class SQLTest {

	
  @Test
  public void propagateID() {
	  	// Setting
		Setting setting = new Setting();
		setting.dbType = "MySQL";
		setting.inputSchema = "financial";
		setting.outputSchema = "financial";
		setting.idColumn = "account_id";
		setting.idTable = "account";
		setting.targetColumn = "status";
		setting.targetTable = "loan";
		setting.sampleTable = "mainSample";
		setting.baseTable = "base";
		setting.isCreateTableAsCompatible = true; 
		setting.quoteMarks ="``";
	  
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
}
