
package connection;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JDBCPropertyListTest {

  
  @Test
  public void getJDBCProperties() {
	  JDBCPropertyList driverList = JDBCPropertyList.unmarshall();  
	  Assert.assertEquals(driverList.getJDBCProperties("MySQL").driverClass, "com.mysql.jdbc.Driver");    
  }
}
