package run;


import org.apache.log4j.Logger;

public class SystemQualityControl {

	// Logging
	private static final Logger logger = Logger.getLogger(SystemQualityControl.class.getName());

	// A set of system variables that are useful for debugging of client's problems.
	protected static void validateSystem(){
		logger.debug("Java Version: " +  System.getProperty("java.version"));
		logger.debug("Predictor Factory Version: " + getVersion());
	}

	// Read the version from the manifest file. If executed from IDE, return "Development".
	private static String getVersion() {
		String version = controller.MainApp.class.getPackage().getImplementationVersion();
		if (version == null) return "Development";
		return version;
	}
}
