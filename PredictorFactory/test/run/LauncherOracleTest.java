package run;

import java.util.SortedMap;

import org.junit.Assert;
import org.junit.Test;

import utility.Meta;
import connection.Network;
import connection.SQL;

public class LauncherOracleTest {
	
	@Test
    public void test_Oracle() {
        String[] arguments = new String[]{"Oracle", "financial_xe_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("Oracle", "financial_xe_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate__100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }
}
