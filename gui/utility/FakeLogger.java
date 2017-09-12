package utility;

import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

// This is a class for faking the calculation of predictors.
public class FakeLogger {
	// Logging
	private static final Logger logger = Logger.getLogger(FakeLogger.class.getName());

	public static void fakeCalculation() {
		try {
			logger.info("Successfully connected to the database");
			TimeUnit.MILLISECONDS.sleep(952);
			logger.info("Collected metadata about the database");
			TimeUnit.MILLISECONDS.sleep(1238);
			logger.info("Propagated the base table to depth: 1");
			TimeUnit.MILLISECONDS.sleep(1323);
			logger.info("Propagated the base table to depth: 2");
			TimeUnit.MILLISECONDS.sleep(1411);
			logger.info("Propagated the base table to depth: 3");
			TimeUnit.MILLISECONDS.sleep(3833);
			logger.info("Produced MAINSAMPLE table with 1000 most predictive predictors");
			logger.info("Finished");

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
