package controller;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;
import run.Launcher;
import utility.Logging;
import utility.ValidatorResource;

public class MainApp extends Application {

	// Main method is only for legacy support - Java 8 won't call it for a JavaFX application (it directly calls launch()).
	public static void main(String[] args) {
		launch(args);   // Tell Java to show the GUI
	}

	@Override
	// Define the GUI. Stage is OS window. Scene is the content of the window.
	// Note: All network connections are automatically closed on window closing - no need to handle it manually.
	public void start(Stage stage) throws Exception {

		// If command line arguments are passed to the GUI, pass them to PredictorFactory core and exit.
		String[] parameters = getParameters().getRaw().toArray(new String[0]);
		if (parameters.length>0) {
			Launcher.main(parameters);
			stage.close();
			Platform.exit();
			return;
		}

		System.out.println("All the resources were found: " + ValidatorResource.isResourceAvailable());

		// Setup logging - load the property file. Important for subsequent connection to the database.
		// SHOULD USE A DIFFERENT CONFIGURATION FILE WITH GUI APPENDER ENABLED
		Logging.initialization();

		Parent root = FXMLLoader.load(getClass().getResource("/fxml/main.fxml"));

		stage.setTitle("Predictor Factory");
		stage.getIcons().add(new Image("/img/ios7-gear-24.png"));
		stage.setMinWidth(600);
		stage.setMinHeight(460);
		stage.setScene(new Scene(root, 600, 460));
		stage.show();
	}
}
