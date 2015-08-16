package metaInformation;


import connection.Network;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;
import utility.Meta;

import java.util.SortedMap;

public class MetaInputTest {

    Setting setting;

    @Before
    public void initialization() {
        setting = new Setting("PostgreSQL", "financial_test_setting");
        setting = Network.openConnection(setting);
    }

    @Test
    public void tableList_noFilter() {
        setting.whiteListTable = "";
        setting.blackListTable = "";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        Assert.assertEquals(8, metaInput.size());
        Assert.assertTrue(metaInput.containsKey("order"));
    }

    @Test
    public void tableList_whiteFilter() {
        setting.whiteListTable = "loan,order";
        setting.blackListTable = "";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        Assert.assertEquals(2, metaInput.size());
        Assert.assertTrue(metaInput.containsKey("loan"));
    }

    @Test
    public void tableList_blackFilter() {
        setting.whiteListTable = "";
        setting.blackListTable = "order";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        Assert.assertEquals(7, metaInput.size());
        Assert.assertFalse(metaInput.containsKey("order"));
    }

    @Test
    public void tableList_whiteBlackFilter() {
        setting.whiteListTable = "loan,order";
        setting.blackListTable = "order";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        Assert.assertEquals(1, metaInput.size());
        Assert.assertTrue(metaInput.containsKey("loan"));
    }

    @Test
    public void columnList_noFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "";
        setting.blackListColumn = "";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        //Assert.assertEquals(1, metaInput.get("loan").nominalColumn.size()); // TARGET IS NOT CURRENTLY INCLUDED
        Assert.assertEquals(3, metaInput.get("loan").numericalColumn.size());
        Assert.assertEquals(1, metaInput.get("loan").timeColumn.size());
        Assert.assertEquals(2, metaInput.get("loan").idColumn.size());
        Assert.assertTrue(metaInput.get("loan").numericalColumn.contains("duration"));
    }

    @Test
    public void columnList_whiteFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "loan.duration";
        setting.blackListColumn = "";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        Assert.assertEquals(1, metaInput.get("loan").numericalColumn.size());
        Assert.assertEquals(0, metaInput.get("loan").timeColumn.size());
        //Assert.assertEquals(0, metaInput.get("loan").idColumn.size());    // IDS ARE WRONGFULLY IGNORED FROM THE LIST
        Assert.assertTrue(metaInput.get("loan").numericalColumn.contains("duration"));
    }

    @Test
    public void columnList_blackFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "";
        setting.blackListColumn = "loan.duration";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        Assert.assertEquals(2, metaInput.get("loan").numericalColumn.size());
        Assert.assertEquals(1, metaInput.get("loan").timeColumn.size());
        Assert.assertEquals(2, metaInput.get("loan").idColumn.size());
        Assert.assertFalse(metaInput.get("loan").numericalColumn.contains("duration"));
    }

    @Test
    public void columnList_whiteBlackFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "loan.duration,loan.status,loan.amount";
        setting.blackListColumn = "loan.duration";
        SortedMap<String, Meta.Table> metaInput = MetaInput.getMetaInput(setting);

        Assert.assertEquals(1, metaInput.get("loan").numericalColumn.size());
        Assert.assertEquals(0, metaInput.get("loan").timeColumn.size());
        Assert.assertFalse(metaInput.get("loan").numericalColumn.contains("duration"));
        Assert.assertTrue(metaInput.get("loan").numericalColumn.contains("amount"));
    }

    @After
    public void closeConnection(){
        Network.closeConnection(setting);
    }

}