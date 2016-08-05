
package connection;

import org.junit.Assert;
import org.junit.Test;



public class DatabasePropertyListTest {
  
  @Test
  public void testReadWriteRead() {
	  DatabasePropertyList databaseList = DatabasePropertyList.unmarshall();  
	  DatabasePropertyList.marshall(databaseList);
	  DatabasePropertyList databaseList2 = DatabasePropertyList.unmarshall();
	  
	  String expected = "account_id";
	  String retrieved = databaseList.getDatabaseProperties("financial").targetId;
	  String retrieved2 = databaseList2.getDatabaseProperties("financial").targetId;
	 
	  Assert.assertEquals(expected, retrieved);
	  Assert.assertEquals(expected, retrieved2);  
  }

	@Test
	public void testAttributeDefault() {
		DatabasePropertyList databaseList = DatabasePropertyList.unmarshall();
		DatabaseProperty databaseProperty = databaseList.getDatabaseProperties("financial");

		// Assumes that the property is not set and the default kicked in
		Assert.assertTrue(databaseProperty.useIdAttributes);
	}



}
