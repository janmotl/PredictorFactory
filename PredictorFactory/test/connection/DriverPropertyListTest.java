
package connection;

import org.junit.Assert;
import org.junit.Test;



public class DriverPropertyListTest {

  @Test
  public void getDriverProperties() {
	  DriverPropertyList driverList = DriverPropertyList.unmarshall();  
	  Assert.assertEquals("com.mysql.jdbc.Driver", driverList.getDriverProperties("MySQL").driverClass);    
  }

}
