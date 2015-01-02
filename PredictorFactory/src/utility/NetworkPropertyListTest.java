
package utility;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NetworkPropertyListTest {

  
  @Test
  public void getJDBCProperties() {
	  NetworkPropertyList driverList = NetworkPropertyList.unmarshall();  
	  Assert.assertEquals(driverList.getJDBCProperties("MySQL").driverClass, "com.mysql.jdbc.Driver");    
  }
}
