package run;


import connection.Network;
import meta.Column;
import org.junit.Assert;
import org.junit.Test;
import utility.Meta;

import java.util.List;
import java.util.SortedMap;

public class LauncherTest {

    @Test
    public void test_Azure() {
        String[] arguments = {"Azure", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("Azure", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTablePrefix);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTablePrefix);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_HSQLDB() {
        String[] arguments = {"HSQLDB", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("HSQLDB", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTablePrefix);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTablePrefix);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_MonetDB() {
        String[] arguments = {"MonetDB", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("MonetDB", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTablePrefix);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTablePrefix);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_MySQL() {
        String[] arguments = {"MariaDB", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("MariaDB", "financial_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTablePrefix);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTablePrefix);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100004"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_numericalColumn_100014"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(14, columnList.size());
    }

    @Test
    public void test_Oracle() {
        String[] arguments = {"Oracle", "financial_xe_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("Oracle", "financial_xe_test_setting");
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, setting.mainTablePrefix);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, setting.mainTablePrefix);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate__100002"));
        Assert.assertTrue(columnList.containsKey("loan_amount_directField_100006"));
        Assert.assertEquals(120, rowCount);
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_PostgreSQL() {
        String[] arguments = {"PostgreSQL", "financial_test_setting"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "financial_test_setting");
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        String sql = "select column_name " +
                "from information_schema.columns " +
                "where table_schema = 'predictor_factory' " +
                "and table_name = '" + mainSample + "'";
        List<String> columnList = Network.executeQuery(setting.dataSource, sql);
        sql = "select temporal_constraint " +
                "from predictor_factory.journal_table " +
                "where original_name = 'trans'";
        List<String> constraintColumn = Network.executeQuery(setting.dataSource, sql);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.contains("trans_amount_aggregate_avg_100004"));
        Assert.assertTrue(columnList.contains("loan_amount_directField_numericalColumn_100014"));
        Assert.assertEquals("loan_date_directField_temporalColumn_100016", columnList.get(3)); // Sorted in desc. order by relevance
        Assert.assertEquals("date", constraintColumn.get(0));    // Was time constrain applied?
        Assert.assertEquals(120, rowCount);     // SampleSize is 30, 4 classes -> 120 samples
        Assert.assertEquals(14, columnList.size()); // 11 features + 3 base
    }


    /////////////////// REGRESSION ///////////////

    @Test
    public void test_regression_Azure() {
        String[] arguments = {"Azure", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("Azure", "financial_test_setting_regression");
	    String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100004"));
        Assert.assertTrue(columnList.containsKey("loan_status_directField_nominalColumn_100011"));
        Assert.assertEquals(30, rowCount);          // sampleCount=30 (it is regression -> 30 in total)
        Assert.assertEquals(15, columnList.size()); // 12 successful predictors + 3 base
    }

    @Test
    public void test_regression_MySQL() {
        String[] arguments = {"MariaDB", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("MariaDB", "financial_test_setting_regression");
	    String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("trans_amount_aggregate_avg_100004"));
        Assert.assertTrue(columnList.containsKey("loan_status_directField_nominalColumn_100011"));
        Assert.assertEquals(30, rowCount);          // sampleCount=30 (it is regression -> 30 in total)
        Assert.assertEquals(15, columnList.size()); // 12 successful predictors + 3 base
    }

    @Test
    public void test_regression_Oracle() {
        String[] arguments = {"Oracle", "financial_xe_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("Oracle", "financial_xe_test_setting_regression");
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
	    Network.openConnection(setting);
        SortedMap<String, Column> columnList = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.containsKey("order_amount_aggregate__100002"));
        Assert.assertTrue(columnList.containsKey("loan_status_directField_100006"));
        Assert.assertEquals(30, rowCount);              // sampleCount=30 (it is regression -> 30 in total)
        Assert.assertEquals(11, columnList.size());
    }

    @Test
    public void test_regression_PostgreSQL() {
        String[] arguments = {"PostgreSQL", "financial_test_setting_regression"};
        Launcher.main(arguments);

        Setting setting = new Setting("PostgreSQL", "financial_test_setting_regression");
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
	    Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        String sql = "select column_name " +
                "from information_schema.columns " +
                "where table_schema = 'predictor_factory' " +
                "and table_name = '" + mainSample + "'";
        List<String> columnList = Network.executeQuery(setting.dataSource, sql);
        Network.closeConnection(setting);

        Assert.assertTrue(columnList.contains("trans_amount_aggregate_avg_100004"));
        Assert.assertTrue(columnList.contains("loan_status_directField_nominalColumn_100011"));
        Assert.assertEquals("loan_date_directField_temporalColumn_100016", columnList.get(3)); // Sorted in desc. order by relevance
        Assert.assertEquals("trans_amount_aggregate_sum_100010", columnList.get(11)); // Sorted in desc. order by relevance
        Assert.assertEquals(30, rowCount);          // sampleCount=30 (it is regression -> 30 in total)
        Assert.assertEquals(15, columnList.size()); // 12 successful predictors + 3 base
    }


    // Test VOC dataset
    @Test
    public void test_compositeKey_PostgreSQL() {
        String[] arguments = {"PostgreSQL", "voc_test_setting"};
        Launcher.main(arguments);

        // Collect validation data
        Setting setting = new Setting("PostgreSQL", "voc_test_setting");
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        Network.closeConnection(setting);

        // Assert
        Assert.assertTrue(columnMap.containsKey("total_death_during_voyage_directField_numericalColumn_100005"));
        Assert.assertTrue(columnMap.containsKey("voyages_master_directField_nominalColumn_100003"));
        Assert.assertTrue(columnMap.containsKey("voyages_cape_departure_timeSince_100011"));
        Assert.assertEquals(520, rowCount);     // Since valueCount=20 only the most common target values make it. Also, sampleCount=30.
        Assert.assertEquals(15, columnMap.size());  // 11 successful predictors + 4 base
    }

    // Test Mutagenesis dataset
    @Test
    public void test_noDate_PostgreSQL() {
        String[] arguments = {"PostgreSQL", "mutagenesis_test_setting"};
        Launcher.main(arguments);

        // Collect validation data
        Setting setting = new Setting("PostgreSQL", "mutagenesis_test_setting");
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        Network.closeConnection(setting);

        // Assert
        Assert.assertTrue(columnMap.containsKey("atom_charge_aggregate_min_100009"));
        Assert.assertTrue(columnMap.containsKey("molecule_logp_directField_numericalColumn_100024"));
        Assert.assertEquals(188, rowCount);         // All rows
        Assert.assertEquals(19, columnMap.size());  // 16 successful predictors + 3 base
    }

}