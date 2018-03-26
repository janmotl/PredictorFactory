package meta;

import connection.Network;
import org.junit.Test;
import run.Setting;
import utility.Meta;

import java.util.SortedMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ColumnTest {

	@Test
	public void containsNull() {
		utility.Logging.initialization();
		Setting setting = new Setting("PostgreSQL", "financial");
		Network.openConnection(setting);
		SortedMap<String, Column> columns = Meta.collectColumns(setting, setting.database, setting.targetSchema, "district");

		assertTrue(columns.get("A12").containsNull(setting));
		assertFalse(columns.get("A2").containsNull(setting));

		Network.closeConnection(setting);
	}

	@Test
	public void containsFutureDate() {
		utility.Logging.initialization();
		Setting setting = new Setting("PostgreSQL", "employee");
		Network.openConnection(setting);
		SortedMap<String, Column> columns = Meta.collectColumns(setting, setting.database, setting.targetSchema, "dept_emp");

		assertTrue(columns.get("to_date").containsFutureDate(setting));
		assertFalse(columns.get("from_date").containsFutureDate(setting));

		Network.closeConnection(setting);
	}

	@Test
	public void getUniqueValues() {
		utility.Logging.initialization();
		Setting setting = new Setting("PostgreSQL", "financial");
		setting.valueCount = 1;
		Network.openConnection(setting);
		SortedMap<String, Column> columns = Meta.collectColumns(setting, setting.database, setting.targetSchema, "district");

		assertTrue(columns.get("A3").getUniqueValues(setting).keySet().contains("south Moravia"));

		Network.closeConnection(setting);
	}

	@Test
	public void isUnique() {
		utility.Logging.initialization();
		Setting setting = new Setting("PostgreSQL", "AdventureWorks");
		setting.whiteListTable = "Address,SalesOrderHeader";
		Network.openConnection(setting);
		Schema schema = new Schema(setting, setting.targetSchema);

		assertTrue(schema.getTable("Address").getColumn("AddressID").isUnique);  // PK constrain
		assertTrue(schema.getTable("Address").getColumn("rowguid").isUnique);    // Unique constrained
		assertFalse(schema.getTable("Address").getColumn("City").isUnique);

		Network.closeConnection(setting);
	}

}