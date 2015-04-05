package connection;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import run.Setting;

public class NetworkTest {
	private Setting setting;
	
	@BeforeTest
	public void connectToDatabase(){
		setting = new Setting();
		setting = Network.getConnection(setting, "MySQL FIT", "financial");
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
		Assert.assertFalse(setting.isSchemaCompatible);
	}
}
