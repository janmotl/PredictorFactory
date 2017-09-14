package utility

import connection.Network
import meta.ForeignConstraint
import run.Setting
import spock.lang.Specification

class MetaSpec extends Specification {

    def "Schemas #connection"() {
        Setting setting = new Setting(connection, database);

        when:
        Network.openConnection(setting);
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Network.closeConnection(setting);

        then:
        schemaList.contains(setting.targetSchema);
		schemaList.contains(setting.outputSchema);

        where:
        connection   | database
        "PostgreSQL" | "financial_test_setting"
        "MariaDB"    | "financial_test_setting"
        "Azure"      | "financial_test_setting"
        "Oracle"     | "financial_xe_test_setting"  // The database name is fixed to be XE
        "SAS"        | "financial_test_setting"
    }

    def "Tables #connection"() {
        Setting setting = new Setting(connection, database);

        when:
        Network.openConnection(setting);
		Set<String> tableList = Meta.collectTables(setting, setting.database, setting.targetSchema).keySet();
		Network.closeConnection(setting);

        then:
        tableList.toString() == "[account, card, client, disp, district, loan, order, trans]";

        where:
        connection   | database
        "PostgreSQL" | "financial_test_setting"
        "MariaDB"    | "financial_test_setting"
        "Azure"      | "financial_test_setting"
        "Oracle"     | "financial_xe_test_setting"  // The database name is fixed to be XE
        "SAS"        | "financial_test_setting"
    }

    def "Columns #connection"() {
        Setting setting = new Setting(connection, database);

        when:
        Network.openConnection(setting);
		Set<String> columnList = Meta.collectColumns(setting, setting.database, setting.targetSchema, "loan").keySet();
		Network.closeConnection(setting);

        then:
        columnList.toString() == "[account_id, amount, date, duration, loan_id, payments, status]";

        where:
        connection   | database
        "PostgreSQL" | "financial_test_setting"
        "MariaDB"    | "financial_test_setting"
        "Azure"      | "financial_test_setting"
        "Oracle"     | "financial_xe_test_setting"  // The database name is fixed to be XE
        "SAS"        | "financial_test_setting"
    }

    def "Primary keys #connection"() {
        Setting setting = new Setting(connection, database);

        when:
        Network.openConnection(setting);
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.targetSchema, "loan");
		Network.closeConnection(setting);

        then:
        primaryKey == "loan_id";

        where:
        connection   | database
        "PostgreSQL" | "financial_test_setting"
        "MariaDB"    | "financial_test_setting"
        "Azure"      | "financial_test_setting"
        "Oracle"     | "financial_xe_test_setting"  // The database name is fixed to be XE
        "SAS"        | "financial_test_setting"
    }

    def "Relations #connection"() {
        Setting setting = new Setting(connection, database);

        when:
        Network.openConnection(setting);
		List<ForeignConstraint> imported = Meta.collectRelationships(setting, setting.targetSchema, "loan");
        List<ForeignConstraint> exported = Meta.collectRelationships(setting, setting.targetSchema, "district");
		Network.closeConnection(setting);

        then:
        imported.size() == 1;
        imported.get(0).table == "loan";
        imported.get(0).fTable == "account";   // This is in THIS --> THAT order. Not in FK --> PK order
        imported.get(0).column.size() == 1;
        imported.get(0).column.get(0) == "account_id";
        imported.get(0).fColumn.size() == 1;
        imported.get(0).fColumn.get(0) == "account_id";

        exported.size() == 2;
        exported.get(0).table == "district";
        exported.get(0).fTable == "account";
        exported.get(0).column.size() == 1;
        exported.get(0).column.get(0) == "district_id";
        exported.get(0).fColumn.size() == 1;
        exported.get(0).fColumn.get(0) == "district_id";

        where:
        connection   | database
        "PostgreSQL" | "financial_test_setting"
        "MariaDB"    | "financial_test_setting"
        "Azure"      | "financial_test_setting"
        "Oracle"     | "financial_xe_test_setting"  // The database name is fixed to be XE
        "SAS"        | "financial_test_setting"
    }

    def "Composite relations #connection"() {
        Setting setting = new Setting(connection, database);

        when:
        Network.openConnection(setting);
		List<ForeignConstraint> relationList = Meta.collectRelationships(setting, setting.targetSchema, "voyages");
		Network.closeConnection(setting);

        then:
        relationList.size() == 7;

        where:
        connection   | database
        "PostgreSQL" | "voc"
        "MariaDB"    | "voc"
    }

}
