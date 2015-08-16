package utility;


import connection.Network;
import org.junit.Assert;
import org.junit.Test;
import run.Setting;

import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

public class MetaTest {
	
	// Warning: If a test fails on the assert, the connection is not closed. That is not good  
	
	/////////////// Tables ///////////////
	
	@Test
	public void testCollectTables_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
		
		Network.closeConnection(setting);
    }

	@Test
	public void testCollectTables_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectTables_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectTables_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		String expected = "[account, card, client, disp, district, loan, order, trans]";
		Assert.assertTrue(expected.equalsIgnoreCase(tableList.toString()));		// SAS capitalizes table names
		
		Network.closeConnection(setting);
    }
	
	/////////////// Columns ///////////////
	
	@Test
	public void testCollectColumns_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		
		SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.keySet().toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectColumns_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		
		SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.keySet().toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectColumns_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		
		SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.keySet().toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectColumns_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		
		SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "LOAN");
		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.keySet().toString());
		
		Network.closeConnection(setting);
    }
	
	/////////////// Schemas ///////////////
	
	@Test
	public void testCollectSchemas_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectSchemas_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectSchemas_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectSchemas_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
		
		Network.closeConnection(setting);
    }

	/////////////// Relations ///////////////
	
	@Test
	public void testCollectRelations_AzureSQL() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		
		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[[account, account_id, account_id]]", relationList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectRelations_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		
		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[[account, account_id, account_id]]", relationList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectRelations_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		
		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[[account, account_id, account_id]]", relationList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectRelations_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		
		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "LOAN");
		String expected = "[[account, account_id, account_id]]";
		Assert.assertTrue(expected.equalsIgnoreCase(relationList.toString()));
		
		Network.closeConnection(setting);
    }
	
	/////////////// Primary Keys ///////////////
	
	@Test
	public void testGetPrimaryKeys_Azure() {
		Setting setting = new Setting("Azure", "financial");		
		Network.openConnection(setting);
		
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("loan_id", primaryKey);
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testGetPrimaryKeys_MySQL() {
		Setting setting = new Setting("MariaDB", "financial");		
		Network.openConnection(setting);
		
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("loan_id", primaryKey);
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testGetPrimaryKeys_PostgreSQL() {
		Setting setting = new Setting("PostgreSQL", "financial");		
		Network.openConnection(setting);
		
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("loan_id", primaryKey);
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testGetPrimaryKeys_SAS() {
		Setting setting = new Setting("SAS", "SAS");		
		Network.openConnection(setting);
		
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("loan_id", primaryKey);
		
		Network.closeConnection(setting);
    }


	/////////// START TEST

//	@Test
//	public void testCollectRelationsComposite_PostgreSQL() {
//		Setting setting = new Setting("PostgreSQL", "voc");
//		Network.openConnection(setting);
//
//		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "craftsmen");
//		Assert.assertEquals("[[account, account_id, account_id]]", relationList.toString());
//
//		Network.closeConnection(setting);
//	}

	/////////// END TEST
}
