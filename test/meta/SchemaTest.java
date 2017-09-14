package meta;


import connection.Network;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.util.ArrayList;
import java.util.SortedMap;

import static org.junit.Assert.*;

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
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        assertEquals(8, metaInput.size());
        assertTrue(metaInput.containsKey("order"));
    }

    @Test
    public void tableList_whiteFilter() {
        setting.whiteListTable = "loan,order";
        setting.blackListTable = "";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        assertEquals(2, metaInput.size());
        assertTrue(metaInput.containsKey("loan"));
    }

    @Test
    public void tableList_blackFilter() {
        setting.whiteListTable = "";
        setting.blackListTable = "order";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        assertEquals(7, metaInput.size());
        assertFalse(metaInput.containsKey("order"));
    }

    @Test
    public void tableList_whiteBlackFilter() {
        setting.whiteListTable = "loan,order";
        setting.blackListTable = "order";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        assertEquals(1, metaInput.size());
        assertTrue(metaInput.containsKey("loan"));
    }

    @Test
    public void columnList_noFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "";
        setting.blackListColumn = "";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        assertEquals(5, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.NOMINAL).size());
        assertTrue(metaInput.get("loan").getColumn("duration") != null);
    }

    @Test
    public void columnList_whiteFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "loan.duration,loan.date,loan.status,loan.account_id";
        setting.blackListColumn = "";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        assertEquals(4, metaInput.get("loan").getColumns().size());
        assertTrue(metaInput.get("loan").getColumn("duration") != null);
        assertTrue(metaInput.get("loan").getColumn("date") != null);
        assertTrue(metaInput.get("loan").getColumn("status") != null);
        assertTrue(metaInput.get("loan").getColumn("account_id") != null);
        assertEquals(2, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.ID).size());

    }

    @Test
    public void columnList_blackFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "";
        setting.blackListColumn = "loan.duration";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        // Assumes ids are permitted for feature generation
        assertEquals(4, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        assertEquals(2, metaInput.get("loan").getColumns(setting, StatisticalType.ID).size());
    }

    @Test
    public void columnList_whiteBlackFilter() {
        setting.whiteListTable = "loan";
        setting.blackListTable = "";
        setting.whiteListColumn = "loan.duration,loan.status,loan.amount,loan.date,loan.account_id";
        setting.blackListColumn = "loan.duration";
        SortedMap<String, Table> metaInput = Schema.getTables(setting, setting.targetSchema, new ArrayList<>());

        assertEquals(2, metaInput.get("loan").getColumns(setting, StatisticalType.NUMERICAL).size());
        assertEquals(1, metaInput.get("loan").getColumns(setting, StatisticalType.TEMPORAL).size());
        assertTrue(metaInput.get("loan").getColumn("amount") != null);
    }

    @After
    public void closeConnection(){
        Network.closeConnection(setting);
    }

}