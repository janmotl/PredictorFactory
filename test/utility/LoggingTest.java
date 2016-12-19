package utility;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class LoggingTest {

	// Logging
	private static final Logger logger = Logger.getLogger(LoggingTest.class.getName());

	@Test
	public void counterTest() {
		utility.Logging.initialization();
		logger.info("ok");

		int infoCount = CountAppender.getCount(Level.INFO);
		int debugCount = CountAppender.getCount(Level.DEBUG);

		Assert.assertEquals(1, infoCount);
		Assert.assertEquals(0, debugCount);
	}

}
