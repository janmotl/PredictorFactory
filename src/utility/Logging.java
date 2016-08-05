package utility;

import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Logging {

	// Setup logging - load the property file
	public static void initialization() {
		try {
			Properties p = new Properties();
			p.load(new FileInputStream("config/log4j.properties"));
			PropertyConfigurator.configure(p);
		} catch (IOException e) {
			System.out.println(e.getMessage());
			System.out.println("The working directory is: " + System.getProperty("user.dir"));
		}
	}
}
