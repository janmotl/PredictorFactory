/**
 * Fail fast: One of the first thing to do when Predictor Factory starts is to perform the basic sanity check.
 */
package run;

import org.apache.log4j.Logger;
import utility.XML;

import java.io.File;

public class InputQualityControl {
	// Logging
	private static final Logger logger = Logger.getLogger(InputQualityControl.class.getName());
	
	// Validate all XMLs in config and pattern directories
	public static void validateConfiguration() {
		// Validate all XML files
		qcPatterns();
		qcConnection();
		qcDatabase();
		qcDriver();
	}
	

	// Subroutine: Check all pattern XMLs in pattern directory - useful when developing new patterns
	private static void qcPatterns() {
		File dir = new File("pattern");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File path : directoryListing) {			
				boolean isValid = XML.isXMLValid("/resources/pattern.xsd", path.toString());
				if (!isValid) {
					logger.warn("Invalid pattern: " + path.toString());
				}
			}
		}
	}
	
	// Subroutine: Validate connection XML - useful when making new connection
	private static void qcConnection() {
		boolean isValid = XML.isXMLValid("/resources/connection.xsd", "config/connection.xml");
		if (!isValid) {
			logger.warn("Invalid /config/connection.xml");
		}
	}
	
	// Subroutine: Validate database XML - useful when making new connection
	private static void qcDatabase() {
		boolean isValid = XML.isXMLValid("/resources/database.xsd", "config/database.xml");
		if (!isValid) {
			logger.warn("Invalid /config/database.xml");
		}
	}
	
	// Subroutine: Validate driver XML - useful when adding support for a new database vendor 
	private static void qcDriver() {
		boolean isValid = XML.isXMLValid("/resources/driver.xsd", "config/driver.xml");
		if (!isValid) {
			logger.warn("Invalid /config/driver.xml");
		}
	}
}
