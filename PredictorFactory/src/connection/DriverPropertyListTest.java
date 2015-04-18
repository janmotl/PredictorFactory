
package connection;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DriverPropertyListTest {

  
  @Test
  public void getDriverProperties() {
	  DriverPropertyList driverList = DriverPropertyList.unmarshall();  
	  Assert.assertEquals(driverList.getDriverProperties("MySQL").driverClass, "com.mysql.jdbc.Driver");    
  }
}
