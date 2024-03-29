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
		assertEquals(188, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_molecule_DOP"));
		assertEquals(4893, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_atom_FPI"));
		assertEquals(5243, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_bond_7AL"));
		assertEquals(5243, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_bond_DRR"));
		SortedMap<String, Column> meta = Meta.collectColumns(setting, setting.database, setting.outputSchema, "propagated_molecule_DOP");
		Network.closeConnection(setting);
		assertTrue(meta.containsKey("propagated_id1"));
		assertTrue(meta.containsKey("propagated_target1"));
		assertTrue(meta.containsKey("lumo"));

		// Validate the returned list
		assertEquals("The list contains: " + result, 4, result.size());
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_molecule_DOP")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_atom_FPI")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_bond_7AL")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_bond_DRR")));

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
		assertEquals(188, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_molecule_DOP"));
		assertEquals(4893, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_atom_FPI"));
		SortedMap<String, Table> metaTables = Meta.collectTables(setting, setting.database, setting.outputSchema);
		assertFalse(metaTables.containsKey("propagated_bond_7AL"));
		assertFalse(metaTables.containsKey("propagated_bond_DRR"));
		SortedMap<String, Column> meta = Meta.collectColumns(setting, setting.database, setting.outputSchema, "propagated_molecule_DOP");
		Network.closeConnection(setting);
		assertTrue(meta.containsKey("propagated_id1"));
		assertTrue(meta.containsKey("propagated_target1"));
		assertTrue(meta.containsKey("lumo"));

		// Validate the returned list
		assertEquals("The list contains: " + result, 2, result.size());
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_molecule_DOP")));
		assertTrue(result.stream().anyMatch(t->t.name.equals("propagated_atom_FPI")));

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
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_loan")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_account")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_disp")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_district")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_order")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_trans")));
		assertFalse(result.stream().anyMatch(t->t.name.startsWith("propagated_card"))); // The join is empty -> absent
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_client")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_client")));

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
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_SalesOrderHeader")));


		// Count of warnings
		assertTrue(CountAppender.getCount(Level.INFO) > 0);     // Check that logging works
		assertEquals(2, CountAppender.getCount(Level.WARN));    // 2 warnings are expected
	}

	@Test
	public void propagateBase_ddl() {
		utility.Logging.initialization();

		Setting setting = new Setting("PostgreSQL", "financial");   // Alternatively use ctu_financial
		setting.sampleCount = 1;    // The minimal size for the minimal runtime
        setting.ignoreDatabaseForeignConstraints = true;    // Ignore FCs from the database (if the FCs in the database are extremely spotty, it is less error-prone to set all FCs in the DDL exclusively)

		setting = Network.openConnection(setting);
		setting.dialect.prepareOutputSchema(setting);
		setting.dialect.getBase(setting);
		Database metaInput = new Database(setting);

		// Overwrite foreign key definitions from the database with the definition from DDL
		List<ForeignConstraint> foreignConstraintList = ForeignConstraintDDL.unmarshall("financial.ddl", setting.targetSchema);
		for (Table table : metaInput.getAllTables()) {
            assertTrue(table.foreignConstraintList.isEmpty());  // Validation that no FKC was imported from the DB
			table.foreignConstraintList = Meta.getTableForeignConstraints(foreignConstraintList, table.name);
			table.foreignConstraintList = Meta.addReverseDirections(table.foreignConstraintList);
		}

		setting.dialect.getSubSampleClassification(setting, metaInput);

		// Run!
		List<OutputTable> result = Propagation.propagateBase(setting, metaInput);

		// Validate the returned list
		assertEquals("The list contains: " + result, 8, result.size()); // One table is (or can be) empty
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_loan")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_account")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_disp")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_district")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_order")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_trans")));
		assertFalse(result.stream().anyMatch(t->t.name.startsWith("propagated_card"))); // The join is empty -> absent
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_client")));
		assertTrue(result.stream().anyMatch(t->t.name.startsWith("propagated_client")));

		// Count of warnings
		assertTrue(CountAppender.getCount(Level.INFO) > 0);     // Check that logging works
		assertEquals(1, CountAppender.getCount(Level.WARN));    // One warning about no relationships is expected
	}
}
