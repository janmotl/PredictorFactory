
package connection;

import junit.framework.Assert;

//import org.junit.Assert;
import org.junit.Test;



public class ConnectionPropertyListTest {
  
  @Test
  public void testReadWriteRead() {
	  ConnectionPropertyList connectionList = ConnectionPropertyList.unmarshall();  
	  ConnectionPropertyList.marshall(connectionList);
	  ConnectionPropertyList connectionList2 = ConnectionPropertyList.unmarshall();
	  
	  String expected = "relational.fit.cvut.cz";
	  String retrieved = connectionList.getConnectionProperties("MariaDB").host;
	  String retrieved2 = connectionList2.getConnectionProperties("MariaDB").host;
	  
	  Assert.assertEquals(expected, retrieved);
	  Assert.assertEquals(expected, retrieved2);  
  }
}
