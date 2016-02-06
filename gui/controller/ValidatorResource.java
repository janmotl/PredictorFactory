package controller;

import java.io.File;
import java.net.URL;



public class ValidatorResource {
	
	// Sanity check that all the resources are at the places where they are expected
	// UGLY COPY&PASTE
	public static boolean isResourceAvailable() {
		File dir = new File("pattern");
		
		if (!dir.exists()) {
			System.out.println("Pattern directory is missing.");
			return false;
		}
		
		dir = new File("config");
		if (!dir.exists()) {
			System.out.println("Config directory is missing.");
			return false;
		}
		
		URL location = ValidatorResource.class.getResource("/img");
		if (location == null) {
			System.out.println("Img resource (package) is missing.");
			return false;
		}
	
		location = ValidatorResource.class.getResource("/fxml");
		if (location == null) {
			System.out.println("Fxml resource (package) is missing.");
			return false;
		}
		
		location = ValidatorResource.class.getResource("/fxml/main.fxml");
		if (location == null) {
			System.out.println("Fxml.main.fxml resource is missing.");
			return false;
		}
		
		
		return true;
	}
}
