package run

import connection.Network
import meta.Column
import org.apache.log4j.Level
import spock.lang.Specification
import utility.CountAppender
import utility.Meta

class LauncherSpec extends Specification {

    def "Classification #database"() {
        String[] arguments = [database, "financial_test_setting"];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting(database, "financial_test_setting");
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);
        String sql = "select temporal_constraint " +
                "from predictor_factory.journal_table " +
                "where original_name = 'trans'";
        List<String> constraintColumn = Network.executeQuery(setting.dataSource, sql);

        columnMap.containsKey("trans_amount_aggregate_avg_100004");
        columnMap.containsKey("loan_amount_directField_numericalColumn_100014");
        constraintColumn.get(0) == "date";          // Was time constraint applied?
        rowCount == 682;                            // We use all the data for comparable results
        columnMap.size() == 13;                     // 10 good features + 3 base
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) == 0;
        CountAppender.getCount(Level.ERROR) == 0;

        cleanup: "the database connection"
        Network.closeConnection(setting);

        where:
        database << ["PostgreSQL", "MariaDB", "Azure", "Oracle", "SAS"]  // From the easiest to the hardest...
    }


    def "Regression #database"() {
        String[] arguments = [database, "financial_test_setting_regression"];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting(database, "financial_test_setting_regression");
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample);
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);

        columnMap.containsKey("trans_amount_aggregate_avg_100004");
        columnMap.containsKey("loan_status_directField_nominalColumn_100011");
        rowCount == 30;                             // sampleCount=30 (it is regression -> 30 in total)
        columnMap.size() == 14;                     // 11 successful predictors + 3 base
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) == 0;
        CountAppender.getCount(Level.ERROR) == 0;

        cleanup: "the database connection"
        Network.closeConnection(setting);

        where:
        database << ["PostgreSQL", "MariaDB", "Azure", "Oracle", "SAS"] // From the easiest to the hardest...
    }
}
