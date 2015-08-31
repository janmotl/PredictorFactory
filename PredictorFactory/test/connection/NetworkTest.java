package connection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.sql.SQLException;
import java.util.List;

public class NetworkTest {
	private Setting setting = new Setting("MariaDB", "financial");
	
	@Before
	public void connectToDatabase(){
		setting = Network.openConnection(setting);
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
		Assert.assertTrue(setting.supportsCatalogs);
		Assert.assertFalse(setting.supportsSchemas);
	}

	// I AM NOT PASSING THIS TEST!
	@Test
	public void properClosing() throws SQLException {
		for (int i = 0; i < 100; i++) {
			Network.closeConnection(setting);
			Network.openConnection(setting);
		}

		Assert.assertTrue(setting.connection.isValid(2));
	}


	@After
	public void closeConnection(){
		Network.closeConnection(setting);
	}
}
