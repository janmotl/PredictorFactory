package propagation;

import connection.Network;
import connection.SQL;
import org.junit.Assert;
import org.junit.Test;
import run.Setting;
import utility.Meta;
import utility.Meta.Table;

import java.util.SortedMap;

public class PropagationTest {

	@Test
	public void propagateBase() {
		Setting setting = new Setting("MariaDB", "mutagenesis");

		setting = Network.openConnection(setting);
		SQL.tidyUp(setting);
		SQL.getBase(setting);
		SortedMap<String, Table> metaInput = metaInformation.MetaInput.getMetaInput(setting);

		// Run!
		Propagation.propagateBase(setting, metaInput);
		
		// Validate
		Assert.assertEquals(188, SQL.getRowCount(setting, setting.outputSchema, "propagated_molecule_001"));
		Assert.assertEquals(4893, SQL.getRowCount(setting, setting.outputSchema, "propagated_atom_002"));
		Assert.assertEquals(5243, SQL.getRowCount(setting, setting.outputSchema, "propagated_bond_003"));
		SortedMap<String, Integer> meta = Meta.collectColumns(setting, setting.database, setting.outputSchema, "propagated_molecule_001");
		Assert.assertTrue(meta.containsKey("propagated_id"));
		Assert.assertTrue(meta.containsKey("propagated_target"));
		Assert.assertTrue(meta.containsKey("lumo"));
	}
}
