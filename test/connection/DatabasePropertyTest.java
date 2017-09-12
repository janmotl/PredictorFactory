
package connection;

import org.junit.Assert;
import org.junit.Test;


public class DatabasePropertyTest {
  
  @Test
  public void cloning() throws CloneNotSupportedException {
	  DatabasePropertyList databaseList = DatabasePropertyList.unmarshall();
	  DatabaseProperty original = databaseList.getDatabaseProperties("financial");
	  DatabaseProperty copy = (DatabaseProperty) original.clone();
	  copy.name = "foo";

	  Assert.assertEquals("financial", original.name);
	  Assert.assertEquals("foo", copy.name);
  }



}
