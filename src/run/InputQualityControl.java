/**
 * Fail fast: One of the first thing to do when Predictor Factory starts is to perform the basic sanity check.
 */
package run;

import extraction.Pattern;
import org.apache.log4j.Logger;
import utility.PatternMap;
import utility.XML;

import java.io.File;
import java.util.SortedMap;

public class InputQualityControl {
	// Logging
	private static final Logger logger = Logger.getLogger(InputQualityControl.class.getName());

	// Validate all XMLs in config and pattern directories
	public static void validateConfiguration() {
		qcPatterns();
		listPatterns();
		qcConnection();
		qcDatabase();
		qcDriver();
		qcForeignConstraint();
	}


	// Subroutine: List the found patterns
	private static void listPatterns() {
		SortedMap<String, Pattern> patternMap = PatternMap.getPatternMap();
		logger.debug("The 'pattern' directory contains " + patternMap.size() + " patterns: " + patternMap.keySet().toString());
	}

	// Subroutine: Check all pattern XMLs in pattern directory - useful when developing new patterns
	private static void qcPatterns() {
		File dir = new File("pattern");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File path : directoryListing) {
				if (!path.isHidden()) {     // Ignore hidden (like .DS_Store) files.
					boolean isValid = XML.isXMLValid("/pattern.xsd", path.toString());
					if (!isValid) {
						logger.warn("Invalid pattern: " + path.toString());
					}
				}
			}
		}
	}

	// Subroutine: Validate connection XML - useful when making new connection
	private static void qcConnection() {
		boolean isValid = XML.isXMLValid("/connection.xsd", "config/connection.xml");
		if (!isValid) {
			logger.warn("Invalid /config/connection.xml");
		}
	}

	// Subroutine: Validate database XML - useful when making new connection
	private static void qcDatabase() {
		boolean isValid = XML.isXMLValid("/database.xsd", "config/database.xml");
		if (!isValid) {
			logger.warn("Invalid /config/database.xml");
		}
	}

	// Subroutine: Validate driver XML - useful when adding support for a new database vendor
	private static void qcDriver() {
		boolean isValid = XML.isXMLValid("/driver.xsd", "config/driver.xml");
		if (!isValid) {
			logger.warn("Invalid /config/driver.xml");
		}
	}

	// Subroutine: Validate foreignConstraint XML - useful when you can't touch the database
	private static void qcForeignConstraint() {
		// This is not a required input -> raise warning only if the file is present and invalid.
		if (new File("config/foreignConstraint.xml").isFile()) {
			boolean isValid = XML.isXMLValid("/foreignConstraint.xsd", "config/foreignConstraint.xml");
			if (!isValid) {
				logger.warn("Invalid /config/foreignConstraint.xml");
			}
		}
	}
}
