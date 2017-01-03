package connection;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class NetworkTest {
	private Setting setting = new Setting("PostgreSQL", "financial");
	
	@Before
	public void connectToDatabase(){
		utility.Logging.initialization();
		setting = Network.openConnection(setting);
	}

	// Would fail on Oracle and other databases when "from dual" syntax is required
	@Test
	public void executeQuery() {
		String sql = "Select 5";
		List<String> columnList = Network.executeQuery(setting.dataSource, sql);

		Assert.assertEquals("5", columnList.get(0));
	}

	@Test
	public void getDatabaseProperties() {
		Assert.assertTrue(setting.supportsCatalogs);
		Assert.assertTrue(setting.supportsSchemas);
	}

	@Test
	public void properClosing() throws SQLException {
		for (int i = 0; i < 20; i++) {
			Network.closeConnection(setting);
			Network.openConnection(setting);
		}

		Assert.assertTrue(setting.dataSource.getConnection().isValid(2));
	}

	@Test
	public void loadTest() {
		for (int i = 0; i < 30; i++) {

			try (Connection connection = setting.dataSource.getConnection();
					ResultSet rs = connection.getMetaData().getSchemas("PredictorFacotry", "%")) {
				while (rs.next()) {
					String schemaName = rs.getString("TABLE_SCHEM");
					System.out.println(schemaName);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

			System.out.println("	iteration: " + i);
		}
	}

	@Test
	public void loadTest_executeQuery() {
		for (int i = 0; i < 30; i++) {

			String sql = "Select 6";
			List<String> columnList = Network.executeQuery(setting.dataSource, sql);
			Assert.assertEquals("6", columnList.get(0));

			System.out.println("	iteration: " + i);
		}
	}

	@Test
	public void loadTest_metaData() {
		for (int i = 0; i < 30; i++) {

			meta.MetaInput.getMetaInput(setting);

			System.out.println("	iteration: " + i);
		}
	}



	@After
	public void closeConnection(){
		Network.closeConnection(setting);
	}
}
