package meta;


import connection.Network;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.util.SortedMap;

public class SchemaTest {

    private Setting setting;

    @Before
    public void initialization() {
        utility.Logging.initialization();
        setting = new Setting("PostgreSQL", "financial_test_setting");
        setting = Network.openConnection(setting);
    }

    @Test
    public void tableList_noFilter() {
        setting.whiteListTable = "";
        setting.blackListTable = "";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        Assert.assertEquals(8, metaInput.size());
        Assert.assertTrue(metaInput.containsKey("order"));
    }

    @Test
    public void tableList_whiteFilter() {
        setting.whiteListTable = "loan,order";
        setting.blackListTable = "";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        Assert.assertEquals(2, metaInput.size());
        Assert.assertTrue(metaInput.containsKey("loan"));
    }

    @Test
    public void tableList_blackFilter() {
        setting.whiteListTable = "";
        setting.blackListTable = "order";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        Assert.assertEquals(7, metaInput.size());
        Assert.assertFalse(metaInput.containsKey("order"));
    }

    @Test
    public void tableList_whiteBlackFilter() {
        setting.whiteListTable = "loan,order";
        setting.blackListTable = "order";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        Assert.assertEquals(1, metaInput.size());
        Assert.assertTrue(metaInput.containsKey("loan"));
    }

    @Test
    public void columnList_noFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "";
        setting.blackListColumn = "";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        Assert.assertEquals(5, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        Assert.assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        Assert.assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.NOMINAL).size());
        Assert.assertTrue(metaInput.get("loan").getColumn("duration") != null);
    }

    @Test
    public void columnList_whiteFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "loan.duration,loan.date,loan.status,loan.account_id";
        setting.blackListColumn = "";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        Assert.assertEquals(2, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        Assert.assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        Assert.assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.ID).size());
        Assert.assertTrue(metaInput.get("loan").getColumn("duration") != null);
    }

    @Test
    public void columnList_blackFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "";
        setting.blackListColumn = "loan.duration";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        // Assumes ids are permitted for feature generation
        Assert.assertEquals(4, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        Assert.assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        Assert.assertEquals(2, metaInput.get("loan").getColumns(setting, StatisticalType.ID).size());
    }

    @Test
    public void columnList_whiteBlackFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "loan.duration,loan.status,loan.amount,loan.date,loan.account_id";
        setting.blackListColumn = "loan.duration";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema);

        Assert.assertEquals(2, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        Assert.assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        Assert.assertTrue(metaInput.get("loan").getColumn("amount") != null);
    }

    @After
    public void closeConnection(){
        Network.closeConnection(setting);
    }

}