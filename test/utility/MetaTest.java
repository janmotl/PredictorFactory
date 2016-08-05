package utility;


import connection.Network;
import metaInformation.ForeignConstraint;
import org.junit.Assert;
import org.junit.Test;
import run.Setting;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;

public class MetaTest {

	/////////////// Tables ///////////////
	
	@Test
	public void testCollectTables_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		Set<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema).keySet();
		Network.closeConnection(setting);

		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
    }

	@Test
	public void testCollectTables_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		Set<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema).keySet();
		Network.closeConnection(setting);

		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
    }
	
	@Test
	public void testCollectTables_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");
		Network.openConnection(setting);
		Set<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema).keySet();
		Network.closeConnection(setting);

		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
    }
	
	@Test
	public void testCollectTables_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		Set<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema).keySet();
		Network.closeConnection(setting);
		String expected = "[account, card, client, disp, district, loan, order, trans]";

		Assert.assertTrue(expected.equalsIgnoreCase(tableList.toString()));		// SAS capitalizes table names
    }
	
	/////////////// Columns ///////////////
	
	@Test
	public void testCollectColumns_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		Set<String> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan").keySet();
		Network.closeConnection(setting);

		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.toString());
    }
	
	@Test
	public void testCollectColumns_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		Set<String> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan").keySet();
		Network.closeConnection(setting);

		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.toString());
    }
	
	@Test
	public void testCollectColumns_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		Set<String> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan").keySet();
		Network.closeConnection(setting);

		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.toString());
    }
	
	@Test
	public void testCollectColumns_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		Set<String> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "LOAN").keySet();
		Network.closeConnection(setting);

		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.toString());
    }
	
	/////////////// Schemas ///////////////
	
	@Test
	public void testCollectSchemas_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Network.closeConnection(setting);

		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
    }
	
	@Test
	public void testCollectSchemas_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Network.closeConnection(setting);

		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
    }
	
	@Test
	public void testCollectSchemas_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Network.closeConnection(setting);

		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
    }
	
	@Test
	public void testCollectSchemas_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Network.closeConnection(setting);

		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
    }

	/////////////// Relations ///////////////

	@Test
	public void testCollectRelations_Azure() {
		Setting setting = new Setting("Azure", "financial");

		Network.openConnection(setting);
		List<ForeignConstraint> relationList = Meta.collectRelationships(setting, setting.inputSchema, "loan");
		Network.closeConnection(setting);

		Assert.assertEquals(1, relationList.size());
    }

	@Test
	public void testCollectRelations_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");

		Network.openConnection(setting);
		List<ForeignConstraint> relationList = Meta.collectRelationships(setting, setting.inputSchema, "loan");
		Network.closeConnection(setting);

		Assert.assertEquals(1, relationList.size());
    }

	@Test
	public void testCollectRelations_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");

		Network.openConnection(setting);
		List<ForeignConstraint> relationList = Meta.collectRelationships(setting, setting.inputSchema, "loan");
		Network.closeConnection(setting);

		Assert.assertEquals(1, relationList.size());
    }

	@Test
	public void testCollectRelations_SAS() {
		Setting setting = new Setting("SAS", "SAS");

		Network.openConnection(setting);
		List<ForeignConstraint> relationList = Meta.collectRelationships(setting, setting.inputSchema, "LOAN");
		Network.closeConnection(setting);

		Assert.assertEquals(1, relationList.size());
    }

	/////////////// Composite Relations ///////////////

	@Test
	public void testCollectCompositeRelations_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "voc");
		Network.openConnection(setting);
		List<ForeignConstraint> relationList = Meta.collectRelationships(setting, setting.inputSchema, "voyages");

		Network.closeConnection(setting);

		Assert.assertEquals(7, relationList.size());
	}



	/////////////// Primary Keys ///////////////
	
	@Test
	public void testGetPrimaryKeys_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Network.closeConnection(setting);

		Assert.assertEquals("loan_id", primaryKey);
    }
	
	@Test
	public void testGetPrimaryKeys_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Network.closeConnection(setting);

		Assert.assertEquals("loan_id", primaryKey);
    }
	
	@Test
	public void testGetPrimaryKeys_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Network.closeConnection(setting);

		Assert.assertEquals("loan_id", primaryKey);
    }
	
	@Test
	public void testGetPrimaryKeys_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Network.closeConnection(setting);

		Assert.assertEquals("loan_id", primaryKey);
    }
}
