package utility;


import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

import org.junit.Assert;
import org.junit.Test;

import run.Setting;
import connection.Network;

public class MetaTest {
	
	/////////////// Tables ///////////////
	
	@Test
	public void testCollectTables_Azure() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "Azure", "financial");
		
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
		
		Network.closeConnection(setting);
    }

	@Test
	public void testCollectTables_MySQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "MariaDB", "financial");
		
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectTables_PostgreSQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "PostgreSQL", "financial");
		
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		Assert.assertEquals("[account, card, client, disp, district, loan, order, trans]", tableList.toString());
		
		Network.closeConnection(setting);
    }
	
	/////////////// Columns ///////////////
	
	@Test
	public void testCollectColumns_Azure() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "Azure", "financial");
		
		SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.keySet().toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectColumns_MySQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "MariaDB", "financial");
		
		SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.keySet().toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectColumns_PostgreSQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "PostgreSQL", "financial");
		
		SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[account_id, amount, date, duration, loan_id, payments, status]", columnList.keySet().toString());
		
		Network.closeConnection(setting);
    }
	
	/////////////// Schemas ///////////////
	
	@Test
	public void testCollectSchemas_Azure() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "Azure", "financial");
		
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectSchemas_MySQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "MariaDB", "financial");
		
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectSchemas_PostgreSQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "PostgreSQL", "financial");
		
		SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		Assert.assertTrue(schemaList.contains(setting.inputSchema));
		Assert.assertTrue(schemaList.contains(setting.outputSchema));
		
		Network.closeConnection(setting);
    }

	/////////////// Relations ///////////////
	
	@Test
	public void testCollectRelations_AzureSQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "Azure", "financial");
		
		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[[account, account_id, account_id]]", relationList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectRelations_MySQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "MariaDB", "financial");
		
		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[[account, account_id, account_id]]", relationList.toString());
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testCollectRelations_PostgreSQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "PostgreSQL", "financial");
		
		List<List<String>> relationList = Meta.collectRelationships(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("[[account, account_id, account_id]]", relationList.toString());
		
		Network.closeConnection(setting);
    }
	
	/////////////// Primary Keys ///////////////
	
	@Test
	public void testGetPrimaryKeys_Azure() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "Azure", "financial");
		
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("loan_id", primaryKey);
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testGetPrimaryKeys_MySQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "MariaDB", "financial");
		
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("loan_id", primaryKey);
		
		Network.closeConnection(setting);
    }
	
	@Test
	public void testGetPrimaryKeys_PostgreSQL() {
		Setting setting = new Setting();		
		Network.openConnection(setting, "PostgreSQL", "financial");
		
		String primaryKey = Meta.getPrimaryKey(setting, setting.database, setting.inputSchema, "loan");
		Assert.assertEquals("loan_id", primaryKey);
		
		Network.closeConnection(setting);
    }
	
}
