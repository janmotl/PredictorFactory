package run

import connection.Network
import groovy.sql.Sql
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
        Collection<String> columns = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample).keySet();
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);

        columns.any {it.matches("trans_amount_aggregate.*")};   // The column name lengths can differ and order of evaluation can differ -> regex
        columns.any {it.matches("loan_amount_directField.*")};
        rowCount == 682;                            // We use all the data for comparable results
        columns.size() == 14;                       // 11 good features + 3 base
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) == 0;
        CountAppender.getCount(Level.ERROR) == 0;

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
        Collection<String> columns = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample).keySet();
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);

        columns.any {it.matches("trans_amount_aggregate.*")};   // The column name lengths can differ
        columns.any {it.matches("loan_status_directField.*")};
        rowCount == 30;                             // sampleCount=30 (it is regression -> 30 in total)
        columns.size() == 14;                       // 11 successful predictors + 3 base
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) == 0;
        CountAppender.getCount(Level.ERROR) == 0;

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
        Collection<String> columns = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample).keySet();

        setting.dialect.getRowCount(setting, setting.outputSchema, mainSample) == rowCount;
        columns.size() == columnCount;
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) <= 1;    // VOC generates 1 warning - it is a property of the dataset
        CountAppender.getCount(Level.ERROR) == 0;

        Network.closeConnection(setting);

        where:
        database                    || rowCount | columnCount
        "voc_test_setting"          || 1218     | 15     // Sampling; 11 successful predictors + 4 base (compound Id)
        "mutagenesis_test_setting"  || 188      | 20     // All data; 18 successful predictors + 2 base (no targetTime)
    }


    def "PostgreSQL tests all patterns #database"() {
        String[] arguments = ["PostgreSQL", database];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) <= 1;    // Expect 1 unsupported data type. But log of negative values... should be expected as well
        CountAppender.getCount(Level.ERROR) == 0;

        where:
        database << [
                "ctu_datatype_benchmark_test_setting", // We avoid {Aggregate frame, Aggregate subgroup, Ratio} because they take a lot of time
                "ctu_datatype_benchmark_ts_test_setting",
                "ctu_datatype_benchmark_date_test_setting"]
    }


    def "PostgreSQL leaking patterns"() {
        String[] arguments = ["PostgreSQL", database];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting("PostgreSQL", database);

        // Following predictors are ok:
        //  1) atom_atom_id_entropy - it is a non-linearly transformed count of atoms in the molecule
        //  2) atom_atom_id_distinctCount - it is identical with the count of atoms in the molecule
        //  3) atom_atom_id_aggregateTextLength_avg - it leaks information about the count of atoms in the molecule
        //  4) atom_atom_id_aggregateTextLength_stdDev_samp - it leaks information about the count of atoms in the molecule
        //  5) atom_atom_id_concentration - it is a non-linearly transformed count of atoms in the molecule
        //  6) molecule_molecule_id_ind1_segmentComparison - it is like ind1
        String sql = "select count(*) as cnt from predictor_factory.journal_predictor where relevance_mutagenic > 3.0 and column_list like '%id%' and pattern_name not in ('Entropy', 'Distinct count', 'Aggregate text length', 'Concentration', 'Segment comparison')"
        Network.openConnection(setting);
        def connection = new Sql(setting.dataSource);
        def tuple = connection.firstRow(sql);
        tuple.getProperty("cnt") == 0;
        Network.closeConnection(setting);

        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) <= 1;    // Expect 1 division by zero (atom_charge_entropyContinuous)
        CountAppender.getCount(Level.ERROR) == 0;

        where:
        database << [
                "mutagenesis_test_setting_leaks"]
    }


    def "TwoStage #connection"() {
        String[] arguments = [connection, database];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting(connection, database);
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        Collection<String> columns = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample).keySet();
        int rowCount = setting.dialect.getRowCount(setting, setting.outputSchema, mainSample);

        columns.any {it.matches("trans_amount_aggregate.*")};   // The column name lengths can differ and order of evaluation can differ -> regex
        columns.any {it.matches("loan_amount_directField.*")};
        rowCount == 682;                            // We use all the data (because of twoPhase processing)
        columns.size() >= 13;                       // 11 good features + 3 base (but base on luck it can be just 10+3)
        CountAppender.getCount(Level.INFO) > 0;     // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) == 0;
        CountAppender.getCount(Level.ERROR) == 0;

        Network.closeConnection(setting);

        where:
        connection   | database                     // From the easiest to work with to the hardest...
        "PostgreSQL" | "financial_test_setting_twoStage"
        "MariaDB"    | "financial_test_setting_twoStage"
        "Azure"      | "financial_test_setting_twoStage"
        "Oracle"     | "financial_xe_test_setting_twoStage"  // The database name is fixed to be XE
        "SAS"        | "financial_test_setting_twoStage"
    }


    def "PostgreSQL twoStage #database"() {
        String[] arguments = [connection, database];

        when: "we start Predictor Factory"
        Launcher.main(arguments);

        then: "we expect a table with the features"
        Setting setting = new Setting(connection, database);
        String mainSample = setting.mainTablePrefix + "_" + setting.targetColumnList.get(0);
        Network.openConnection(setting);
        Collection<String> columns = Meta.collectColumns(setting, setting.database, setting.outputSchema, mainSample).keySet();

        // The count of rows should equal the count of rows in the target schema
        setting.dialect.getRowCount(setting, setting.outputSchema, mainSample) == setting.dialect.getRowCount(setting, setting.targetSchema, setting.targetTable);

        CountAppender.getCount(Level.INFO) > 0;             // We have to make sure the CountAppender is working
        CountAppender.getCount(Level.WARN) <= warnCount;    // E.g. VOC dataset contains null in the targetDate
        CountAppender.getCount(Level.ERROR) == 0;

        // predictorMax (commonly we reach the limit because it is set to 10) + targets + ids + date
        columns.size() == setting.predictorMax + setting.targetColumnList.size() + setting.targetIdList.size() + (setting.targetDate==null?0:1);

        Network.closeConnection(setting);

        where:
        connection   | database                                     | warnCount
        "PostgreSQL" | "mutagenesis_test_setting_twoStage"          | 0
        "PostgreSQL" | "financial_test_setting_twoStage_tricky"     | 0
        "PostgreSQL" | "world_test_setting_twoStage"                | 0
        "PostgreSQL" | "Biodegradability_test_setting_twoStage"     | 0
//        "PostgreSQL" | "voc_test_setting_twoStage"                  | 1   // Warn: Target date column 'arrival_date' contains null
//        "PostgreSQL" | "PTE_test_setting_twoStage"                  | 0   // The base table contains duplicate values in BaseID
//        "PostgreSQL" | "FNHK_test_setting_twoStage"                 | 0   // FUCKED time constraints




    }
}
