package propagation;

import java.util.SortedMap;

import org.testng.annotations.Test;

import run.Setting;
import utility.Meta.Table;
import connection.Network;

public class PropagationTest {

	@Test
	public void propagateBase() {
		Setting setting = new Setting();

		setting = Network.getConnection(setting, "MySQL", "financial");
		SortedMap<String, Table> metaInput = metaInformation.MetaInput.getMetaInput(setting);

		// Run!
		Propagation.propagateBase(setting, metaInput);
		System.out.println("#### Done ####");
	}
}
