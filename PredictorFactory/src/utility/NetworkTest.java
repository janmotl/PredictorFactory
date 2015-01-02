package utility;

import java.util.ArrayList;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.Assert;

import run.Setting;

public class NetworkTest {
	private Setting setting;
	
	@BeforeTest
	public void connectToDatabase(){
		setting = new Setting();
		setting.dbType = "MySQL";
		setting = Network.getConnection(setting);
	}

	// Would fail on Oracle and other databases when "from dual" syntax is required
	@Test
	public void executeQuery() {
		String sql = "Select 5";
		ArrayList<String> columnList = Network.executeQuery(setting.connection, sql);

		Assert.assertEquals("5", columnList.get(0));
	}

	// Would fail on databases with schema support
	@Test
	public void getDatabaseProperties() {
		Assert.assertFalse(setting.isSchemaCompatible);
	}
}
