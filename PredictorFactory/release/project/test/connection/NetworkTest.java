package connection;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import run.Setting;

public class NetworkTest {
	private Setting setting = new Setting();
	
	@Before
	public void connectToDatabase(){
		setting = Network.openConnection(setting, "MariaDB", "financial");
	}

	// Would fail on Oracle and other databases when "from dual" syntax is required
	@Test
	public void executeQuery() {
		String sql = "Select 5";
		List<String> columnList = Network.executeQuery(setting.connection, sql);

		Assert.assertEquals("5", columnList.get(0));
	}

	// Would fail on databases with schema support
	@Test
	public void getDatabaseProperties() {
		Assert.assertFalse(setting.supportsCatalogs);
	}
}
