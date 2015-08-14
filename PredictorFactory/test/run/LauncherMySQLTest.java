package run;

import java.util.SortedMap;

import org.junit.Assert;
import org.junit.Test;

import utility.Meta;
import connection.Network;
import connection.SQL;

public class LauncherMySQLTest {
	@Test
    public void test_MySQL() {
        String[] arguments = new String[]{"MariaDB", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("MariaDB", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }
}
