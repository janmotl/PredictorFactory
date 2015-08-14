package run;


import connection.Network;
import connection.SQL;

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
        SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate_avg_100002"));
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
        SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate_avg_100002"));
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
        SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
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
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        String sql = "select column_name " +
                "from information_schema.columns " +
                "where table_schema = 'predictor_factory' " +
                "and table_name = 'mainSample'";
        List<String> columnList = Network.executeQuery(setting.connection, sql);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.contains("order_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.contains("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals("loan_date_directField_timeColumn_100008", columnList.get(3)); // Sorted in desc. order by relevance
        Assert.assertEquals("order_amount_aggregate_min_100003", columnList.get(6)); // Sorted in desc. order by relevance
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }


    @Test
    public void test_regression_MySQL() {
        String[] arguments = new String[]{"MariaDB", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("MariaDB", "financial_test_setting_regression");
        Network.openConnection(setting);
        SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate_avg_100002"));
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
        SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.sampleTable);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate__100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }
    
    @Test
    public void test_regression_PostgreSQL() {
        String[] arguments = new String[]{"PostgreSQL", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "financial_test_setting_regression");
        Network.openConnection(setting);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        String sql = "select column_name " +
                "from information_schema.columns " +
                "where table_schema = 'predictor_factory' " +
                "and table_name = 'mainSample'";
        List<String> columnList = Network.executeQuery(setting.connection, sql);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.contains("order_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.contains("loan_status_directField_nominalColumn_100006"));
        Assert.assertEquals("loan_date_directField_timeColumn_100008", columnList.get(7)); // Sorted in desc. order by relevance
        Assert.assertEquals("order_amount_aggregate_min_100003", columnList.get(3)); // Sorted in desc. order by relevance
        Assert.assertEquals(30, rowCount);
        Assert.assertEquals(11, columnList.size());
    }


    // Test VOC dataset
    @Test
    public void test_composite_key_PostgreSQL() {
        String[] arguments = new String[]{"PostgreSQL", "voc_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "voc_test_setting");
        Network.openConnection(setting);
        int rowCount = SQL.getRowCount(setting, setting.outputSchema, setting.sampleTable);
        String sql = "select column_name " +
                "from information_schema.columns " +
                "where table_schema = 'predictor_factory' " +
                "and table_name = 'mainSample'";
        List<String> columnList = Network.executeQuery(setting.connection, sql);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.contains("total_death_during_voyage_directField_numericalColumn_100002"));
        Assert.assertTrue(columnList.contains("voyages_master_directField_nominalColumn_100001"));
        Assert.assertTrue(columnList.contains("voyages_cape_departure_timeSinceDirect_100006"));
        Assert.assertEquals(637, rowCount);
        Assert.assertEquals(10, columnList.size());
    }

}