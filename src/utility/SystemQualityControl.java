package utility;


import com.sun.javafx.runtime.VersionInfo;
import controller.MainApp;
import org.apache.log4j.Logger;

public class SystemQualityControl {

	// Logging
	private static final Logger logger = Logger.getLogger(SystemQualityControl.class.getName());

	// A set of system variables that are useful for debugging of client's problems.
	public static void validateSystem() {
		logger.debug("Predictor Factory Version: " + getPFVersion());
		logger.debug("Java vendor: " + System.getProperty("java.vendor"));
		logger.debug("Java version: " + System.getProperty("java.version"));
		logger.debug("JavaFX version: " + VersionInfo.getRuntimeVersion());
		logger.debug("OS: " + System.getProperty("os.name"));
		logger.debug("Architecture: " + System.getProperty("os.arch"));
	}

	// Read the version from the manifest file. If executed from IDE, return "Development".
	public static String getPFVersion() {
		String version = MainApp.class.getPackage().getImplementationVersion();
		if (version == null) return "Development";
		return version;
	}
}
