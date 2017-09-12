package controller;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.image.Image;
import javafx.stage.Stage;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class Development extends Application {
	@Override
	public void start(Stage stage) throws IOException {

		Parent root = FXMLLoader.load(getClass().getResource("/fxml/development.fxml"));

		stage.setTitle("Predictor Factory");
		stage.getIcons().add(new Image("/img/ios7-gear-24.png"));
		stage.setMinWidth(600);
		stage.setMinHeight(460);
		stage.setScene(new Scene(root, 600, 460));
		stage.show();


	}
}
