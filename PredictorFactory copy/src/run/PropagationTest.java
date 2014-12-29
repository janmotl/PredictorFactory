package run;

import org.testng.annotations.Test;

import utility.Network;

public class PropagationTest {

  @Test
  public void propagateBase() {
	  Setting setting = new Setting();

		setting.dbType = "MySQL";
		setting.isCreateTableAsCompatible = true; 
		setting.quoteMarks ="``";
		
		setting.inputSchema = "financial";
		setting.outputSchema = "financial";
		
		setting.idColumn = "account_id";
		setting.idTable = "account";
		setting.targetDate = "date";
		setting.targetColumn = "status";
		setting.targetTable = "loan";
		
		setting.baseId = "propagated_id";
		setting.baseDate = "propagated_date";
		setting.baseTarget = "propagated_target";
		setting.baseTable = "base";
		
		setting.sampleTable = "mainSample";
		setting.journalTable = "journal";
		setting.propagatedPrefix = "propagated_";
		
		setting = Network.getConnection(setting);
		
		
		// Run!
		Propagation.propagateBase(setting);
		System.out.println("#### Done ####");
  }
}
