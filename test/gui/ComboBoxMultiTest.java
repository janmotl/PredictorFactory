package gui;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.ListView;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import meta.Column;
import utility.ComboBoxMulti;

import java.util.LinkedHashMap;

public class ComboBoxMultiTest extends Application {
	final ListView<String> selectedItems = new ListView<>();

	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage primaryStage) {
		LinkedHashMap<String, Column> columnMap = new LinkedHashMap<>();
		columnMap.put("Apfel", null);
		columnMap.put("Banane", null);
		columnMap.put("Birne", null);
		columnMap.put("Kiwi", null);
		ComboBoxMulti comboBoxMulti = new ComboBoxMulti();
		comboBoxMulti.setAll(columnMap);
		comboBoxMulti.setPrefWidth(300);

		BorderPane borderPane = new BorderPane();
		borderPane.setTop(comboBoxMulti);
		borderPane.setCenter(selectedItems);
		primaryStage.setScene(new Scene(borderPane, 400, 300));
		primaryStage.show();
	}
}