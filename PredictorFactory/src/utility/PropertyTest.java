package utility;

import java.util.Properties;

import org.testng.annotations.Test;
import org.testng.Assert;

public class PropertyTest {

	@Test
  public void getProperties() {
	  Properties config = utility.Property.getProperties();
	  
	  System.out.println(config.getProperty("readServer.schema"));
	  Assert.assertEquals("finance", config.getProperty("readServer.schema"));
	  
	  int number = Integer.valueOf(config.getProperty("predictor.start"));
	  System.out.println(number);
	  Assert.assertEquals(1000000, number);

  }
}
