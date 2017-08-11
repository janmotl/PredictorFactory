package run;


import connection.Network;
import meta.Column;
import org.junit.Assert;
import org.junit.Test;
import utility.Meta;

import java.util.List;
import java.util.SortedMap;

public class LauncherTest {


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