package propagation;

import connection.Network;
import meta.*;
import org.apache.log4j.Level;
import org.junit.Test;
import run.Setting;
import utility.CountAppender;
import utility.Meta;

import java.util.List;
import java.util.SortedMap;

import static org.junit.Assert.*;

public class PropagationTest {

	@Test
	public void propagateBase() {
		utility.Logging.initialization();

		Setting setting = new Setting("PostgreSQL", "mutagenesis_test_setting");
		setting.sampleCount = 10000;

		setting = Network.openConnection(setting);
		setting.dialect.prepareOutputSchema(setting);
		setting.dialect.getBase(setting);
		Database metaInput = new Database(setting);
		setting.dialect.getSubSampleClassification(setting, metaInput);

		// Run!
		List<OutputTable> result = Propagation.propagateBase(setting, metaInput);
		
		// Validate in database
		assertEquals(188, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_molecule_001"));
		assertEquals(4893, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_atom_002"));
		assertEquals(5243, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_bond_003"));
		assertEquals(5243, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_bond_004"));
		SortedMap<String, Column> meta = Meta.collectColumns(setting, setting.database, setting.outputSchema, "propagated_molecule_001");
		Network.closeConnection(setting);
		assertTrue(meta.containsKey("propagated_id1"));
		assertTrue(meta.containsKey("propagated_target1"));
		assertTrue(meta.containsKey("lumo"));

		// Validate the returned list
		assertEquals("The list contains: " + result, 4, result.size());
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_molecule_001")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_atom_002")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_bond_003")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_bond_004")));

		// Count of warnings
		assertTrue(CountAppender.getCount(Level.INFO) > 0);     // Check that logging works
		assertEquals(0, CountAppender.getCount(Level.WARN));
	}

	@Test
	public void propagateBase_depthLimit() {
		utility.Logging.initialization();

		Setting setting = new Setting("PostgreSQL", "mutagenesis_test_setting");
		setting.sampleCount = 10000;
		setting.propagationDepthMax = 2;

		setting = Network.openConnection(setting);
		setting.dialect.prepareOutputSchema(setting);
		setting.dialect.getBase(setting);
		Database metaInput = new Database(setting);
		setting.dialect.getSubSampleClassification(setting, metaInput);

		// Run!
		List<OutputTable> result = Propagation.propagateBase(setting, metaInput);

		// Validate in database
		assertEquals(188, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_molecule_001"));
		assertEquals(4893, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_atom_002"));
		SortedMap<String, Table> metaTables = Meta.collectTables(setting, setting.database, setting.outputSchema);
		assertFalse(metaTables.containsKey("propagated_bond_003"));
		assertFalse(metaTables.containsKey("propagated_bond_004"));
		SortedMap<String, Column> meta = Meta.collectColumns(setting, setting.database, setting.outputSchema, "propagated_molecule_001");
		Network.closeConnection(setting);
		assertTrue(meta.containsKey("propagated_id1"));
		assertTrue(meta.containsKey("propagated_target1"));
		assertTrue(meta.containsKey("lumo"));

		// Validate the returned list
		assertEquals("The list contains: " + result, 2, result.size());
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_molecule_001")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_atom_002")));

		// Count of warnings
		assertTrue(CountAppender.getCount(Level.INFO) > 0);     // Check that logging works
		assertEquals(1, CountAppender.getCount(Level.WARN));    // One warning about not propagating "bond" table is expected
	}

	@Test
	public void propagateBase_emptyTable() {
		utility.Logging.initialization();

		Setting setting = new Setting("PostgreSQL", "financial");
		setting.sampleCount = 1;    // The minimal size for the minimal runtime

		setting = Network.openConnection(setting);
		setting.dialect.prepareOutputSchema(setting);
		setting.dialect.getBase(setting);
		Database metaInput = new Database(setting);
		setting.dialect.getSubSampleClassification(setting, metaInput);

		// Run!
		List<OutputTable> result = Propagation.propagateBase(setting, metaInput);

		// Validate the returned list
		assertEquals("The list contains: " + result, 8, result.size()); // One table is (or can be) empty due to sampleCount==1
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_loan_001")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_account_002")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_disp_003")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_district_004")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_order_005")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_trans_006")));
		assertFalse(result.stream().anyMatch(t->t.name.equals("propagated_card_007"))); // The join is empty -> absent
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_client_008")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_client_009")));

		// Count of warnings
		assertTrue(CountAppender.getCount(Level.INFO) > 0);     // Check that logging works
		assertEquals(0, CountAppender.getCount(Level.WARN));
	}

	@Test
	public void propagateBase_islandTable() {
		utility.Logging.initialization();

		Setting setting = new Setting("PostgreSQL", "AdventureWorks");
		setting.sampleCount = 1;    // The minimal size for the minimal runtime
		setting.whiteListTable = "ErrorLog,SalesOrderHeader";

		setting = Network.openConnection(setting);
		setting.dialect.prepareOutputSchema(setting);
		setting.dialect.getBase(setting);
		Database metaInput = new Database(setting);
		setting.dialect.getSubSampleClassification(setting, metaInput);

		// Run!
		List<OutputTable> result = Propagation.propagateBase(setting, metaInput);

		// Validate the returned list
		assertEquals("The list contains: " + result, 1, result.size()); // Just the target table is expected
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_SalesOrderHeader_001")));


		// Count of warnings
		assertTrue(CountAppender.getCount(Level.INFO) > 0);     // Check that logging works
		assertEquals(2, CountAppender.getCount(Level.WARN));    // 2 warnings are expected
	}

	@Test
	public void propagateBase_ddl() {
		utility.Logging.initialization();

		Setting setting = new Setting("PostgreSQL", "financial");   // Alternatively use ctu_financial
		setting.sampleCount = 1;    // The minimal size for the minimal runtime

		setting = Network.openConnection(setting);
		setting.dialect.prepareOutputSchema(setting);
		setting.dialect.getBase(setting);
		Database metaInput = new Database(setting);

		// Overwrite foreign key definitions from the database with the definition from DDL
		for (Table table : metaInput.getAllTables()) {
			table.foreignConstraintList = ForeignConstraintDDL.getForeignConstraintList("financial.ddl", table.name, setting.targetSchema);
			table.foreignConstraintList = Meta.addReverseDirections(table.foreignConstraintList);
			System.out.println(table.foreignConstraintList);
		}

		setting.dialect.getSubSampleClassification(setting, metaInput);

		// Run!
		List<OutputTable> result = Propagation.propagateBase(setting, metaInput);

		// Validate the returned list
		assertEquals("The list contains: " + result, 8, result.size()); // One table is (or can be) empty
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_loan_001")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_account_002")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_disp_003")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_district_004")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_order_005")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_trans_006")));
		assertFalse(result.stream().anyMatch(t->t.name.equals("propagated_card_007"))); // The join is empty -> absent
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_client_008")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_client_009")));

		// Count of warnings
		assertTrue(CountAppender.getCount(Level.INFO) > 0);     // Check that logging works
		assertEquals(0, CountAppender.getCount(Level.WARN));
	}

	// TODO
//	@Test
//	public void getMatches() throws Exception {
//		Table t1 = new Table();
//		t1.name = "t1";
//		t1.schemaName = "schema";
//
//		Table t2 = new Table();
//		t2.name = "t2";
//		t2.schemaName = "schema";
//
//		Table t3 = new Table();
//		t3.name = "t3";
//		t3.schemaName = "schema";
//
//		List<String> t1Columns = new ArrayList<>();
//		t1Columns.add("t1c1");
//		List<String> t2Columns = new ArrayList<>();
//		t2Columns.add("t2c1");
//		ForeignConstraint fc = new ForeignConstraint("name", "schema", "t1", "schema", "t2", t1Columns, t2Columns);
//		t1.foreignConstraintList.add(fc);
//
//		// Asserts
//		assertEquals(0, Propagation.getMatches(t1, t3).size()); // t1 -/-> t3
//		assertEquals(1, Propagation.getMatches(t1, t2).size()); // t1 ---> t2
//		assertEquals(0, Propagation.getMatches(t2, t1).size()); // t2 -/-> t1
//	}
}
