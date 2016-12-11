package run;


import connection.Network;
import connection.SQL;
import metaInformation.Column;
import org.junit.Assert;
import org.junit.Test;
import utility.Meta;

import java.util.List;
import java.util.SortedMap;

public class LauncherTest {

    @Test
    public void test_Azure() {
        String[] arguments = new String[]{"Azure", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("Azure", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_HSQLDB() {
        String[] arguments = new String[]{"HSQLDB", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("HSQLDB", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_MonetDB() {
        String[] arguments = new String[]{"MonetDB", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("MonetDB", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_MySQL() {
        String[] arguments = new String[]{"MariaDB", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("MariaDB", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_Oracle() {
        String[] arguments = new String[]{"Oracle", "financial_xe_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("Oracle", "financial_xe_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate__100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }
    
    @Test
    public void test_PostgreSQL() {
        String[] arguments = new String[]{"PostgreSQL", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "financial_test_setting");
        Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        String sql = "select column_name " +
                "from information_schema.columns " +
                "where table_schema = 'predictor_factory' " +
                "and table_name = 'mainSample'";
        List<String> columnList = Network.executeQuery(setting.dataSource, sql);
        sql = "select date_constrain " +
                "from predictor_factory.journal_table " +
                "where original_name = 'trans'";
        List<String> constrainColumn = Network.executeQuery(setting.dataSource, sql);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.contains("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.contains("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals("loan_date_directField_timeColumn_100008", columnList.get(3)); // Sorted in desc. order by relevance
        Assert.assertEquals("trans_amount_aggregate_min_100003", columnList.get(10)); // Sorted in desc. order by relevance
        Assert.assertEquals("date", constrainColumn.get(0));    // Was time constrain applied?
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }


    /////////////////// REGRESSION ///////////////

    @Test
    public void test_regression_Azure() {
        String[] arguments = new String[]{"Azure", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("Azure", "financial_test_setting_regression");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_status_directField_nominalColumn_100006"));
        Assert.assertEquals(30, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_regression_MySQL() {
        String[] arguments = new String[]{"MariaDB", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("MariaDB", "financial_test_setting_regression");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_status_directField_nominalColumn_100006"));
        Assert.assertEquals(30, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_regression_Oracle() {
        String[] arguments = new String[]{"Oracle", "financial_xe_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("Oracle", "financial_xe_test_setting_regression");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate__100002"));
        Assert.assertTrue(columnList.containsKey("loan_status_directField_100006"));
        Assert.assertEquals(30, rowCount);
        Assert.assertEquals(11, columnList.size());
    }
    
    @Test
    public void test_regression_PostgreSQL() {
        String[] arguments = new String[]{"PostgreSQL", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "financial_test_setting_regression");
        Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        String sql = "select column_name " +
                "from information_schema.columns " +
                "where table_schema = 'predictor_factory' " +
                "and table_name = 'mainSample'";
        List<String> columnList = Network.executeQuery(setting.dataSource, sql);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.contains("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.contains("loan_status_directField_nominalColumn_100006"));
        Assert.assertEquals("loan_date_directField_timeColumn_100008", columnList.get(8)); // Sorted in desc. order by relevance
        Assert.assertEquals("trans_amount_aggregate_sum_100005", columnList.get(7)); // Sorted in desc. order by relevance
        Assert.assertEquals(30, rowCount);
        Assert.assertEquals(11, columnList.size());
    }


    // Test VOC dataset
    @Test
    public void test_compositeKey_PostgreSQL() {
        String[] arguments = new String[]{"PostgreSQL", "voc_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "voc_test_setting");
        Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("total_death_during_voyage_directField_numericalColumn_100002"));
        Assert.assertTrue(columnList.containsKey("voyages_master_directField_nominalColumn_100001"));
        Assert.assertTrue(columnList.containsKey("voyages_cape_departure_timeSinceDirect_100006"));
        Assert.assertEquals(637, rowCount);
        Assert.assertEquals(10, columnList.size());
    }

    // Test Mutagenesis dataset
    @Test
    public void test_noDate_PostgreSQL() {
        String[] arguments = new String[]{"PostgreSQL", "mutagenesis_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "mutagenesis_test_setting");
        Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTable);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("bond_type_aggregate_max_100015"));
        Assert.assertTrue(columnList.containsKey("molecule_logp_directField_numericalColumn_100023"));
        Assert.assertTrue(columnList.containsKey("atom_charge_aggregate_sum_100017"));
        Assert.assertEquals(188, rowCount);
        Assert.assertEquals(26, columnList.size());
    }

}