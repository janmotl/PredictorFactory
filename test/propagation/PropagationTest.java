package propagation;

import connection.Network;
import meta.Column;
import meta.Table;
import org.junit.Assert;
import org.junit.Test;
import run.Setting;
import utility.Meta;

import java.util.SortedMap;

public class PropagationTest {

	@Test
	public void propagateBase() {
		utility.Logging.initialization();

		Setting setting = new Setting("PostgreSQL", "mutagenesis");
		setting.sampleCount = 10000;

		setting = Network.openConnection(setting);
		setting.dialect.prepareOutputSchema(setting);
		setting.dialect.getBase(setting);
		SortedMap<String, Table> metaInput = meta.MetaInput.getMetaInput(setting);
		setting.dialect.getSubSampleClassification(setting, metaInput);

		// Run!
		Propagation.propagateBase(setting, metaInput);
		
		// Validate
		Assert.assertEquals(188, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_molecule_001"));
		Assert.assertEquals(4893, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_atom_002"));
		Assert.assertEquals(5243, setting.dialect.getRowCount(setting, setting.outputSchema, "propagated_bond_003"));
		SortedMap<String, Column> meta = Meta.collectColumns(setting, setting.database, setting.outputSchema, "propagated_molecule_001");
		Network.closeConnection(setting);
		Assert.assertTrue(meta.containsKey("propagated_id1"));
		Assert.assertTrue(meta.containsKey("propagated_target1"));
		Assert.assertTrue(meta.containsKey("lumo"));
	}
}
