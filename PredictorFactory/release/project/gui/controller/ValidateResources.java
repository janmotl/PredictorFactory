package controller;

import java.io.File;
import java.net.URL;



public class ValidateResources {
	
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
		
		URL location = ValidateResources.class.getResource("/img");
		if (location == null) {
			System.out.println("Img resource (package) is missing.");
			return false;
		}
	
		location = ValidateResources.class.getResource("/view");
		if (location == null) {
			System.out.println("View resource (package) is missing.");
			return false;
		}
		
		location = ValidateResources.class.getResource("/view/All.fxml");
		if (location == null) {
			System.out.println("View.All.fxml resource is missing.");
			return false;
		}
		
		
		return true;
	}
}
