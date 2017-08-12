package run

import connection.Network
import meta.Column
import org.apache.log4j.Level
import spock.lang.Specification
import utility.CountAppender
import utility.Meta

class LauncherSpec extends Specification {

    def "Classification #connection"() {
        String[] arguments = [connection, database];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting(connection, database);
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);

        columnMap.any {it.key.matches("trans_amount_aggregate.*100004")};   // The column name lengths can differ
        columnMap.any {it.key.matches("loan_amount_directField.*100014")};
        rowCount == 682;                            // We use all the data for comparable results
        columnMap.size() == 13;                     // 10 good features + 3 base
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) == 0;
        CountAppender.getCount(Level.ERROR) == 0;

        cleanup: "the database connection"
        Network.closeConnection(setting);

        where:
        connection   | database                     // From the easiest to work with to the hardest...
        "PostgreSQL" | "financial_test_setting"
        "MariaDB"    | "financial_test_setting"
        "Azure"      | "financial_test_setting"
        "Oracle"     | "financial_xe_test_setting"  // The database name is fixed to be XE
        "SAS"        | "financial_test_setting"
    }


    def "Regression #connection"() {
        String[] arguments = [connection, database];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting(connection, database);
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);

        columnMap.any {it.key.matches("trans_amount_aggregate.*100004")};   // The column name lengths can differ
        columnMap.any {it.key.matches("loan_status_directField.*100011")};
        rowCount == 30;                             // sampleCount=30 (it is regression -> 30 in total)
        columnMap.size() == 14;                     // 11 successful predictors + 3 base
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) == 0;
        CountAppender.getCount(Level.ERROR) == 0;

        cleanup: "the database connection"
        Network.closeConnection(setting);

        where:
        connection   | database                                 // From the easiest to the hardest...
        "PostgreSQL" | "financial_test_setting_regression"
        "MariaDB"    | "financial_test_setting_regression"
        "Azure"      | "financial_test_setting_regression"
        "Oracle"     | "financial_xe_test_setting_regression"   // The database name is fixed to be XE
        "SAS"        | "financial_test_setting_regression"
    }


    def "PostgreSQL #database"() {
        String[] arguments = ["PostgreSQL", database];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting("PostgreSQL", database);
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);

        setting.dialect.getRowCount(setting, setting.outputSchema, mainSample) == rowCount;
        columnMap.size() == columnCount;
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) <= 1;    // VOC generates 1 warning - it is a property of the dataset
        CountAppender.getCount(Level.ERROR) == 0;

        cleanup: "the database connection"
        Network.closeConnection(setting);

        where:
        database                    || rowCount | columnCount
        "voc_test_setting"          || 1218     | 15     // Sampling; 11 successful predictors + 4 base (compound Id)
        "mutagenesis_test_setting"  || 188      | 18     // All data; 16 successful predictors + 2 base (no targetTime)
    }
}
