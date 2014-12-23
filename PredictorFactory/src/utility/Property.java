package utility;

// Source:
// 	http://viralpatel.net/blogs/loading-java-properties-files

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class Property {
	
	// Return the property object
	public static Properties getProperties(){
		Properties config = new Properties();
		
		InputStream in = Property.class.getClassLoader().getResourceAsStream("resources/config.properties");
		try {
			config.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return config;
	}
}
