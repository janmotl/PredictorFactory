package extraction;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.ListView;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import org.controlsfx.control.CheckComboBox;

public class SandboxCheckComboBox extends Application {
	final ListView<String> selectedItems = new ListView<>();

	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage primaryStage) {
		ObservableList<String> columnList = FXCollections.observableArrayList();
		columnList.add("Apfel");
		columnList.add("Banane");
		columnList.add("Birne");
		columnList.add("Kiwi");
		CheckComboBox<String> checkComboBox = new CheckComboBox<>();
		checkComboBox.getItems().setAll(columnList);
		checkComboBox.setPrefWidth(300);


		BorderPane borderPane = new BorderPane();
		borderPane.setTop(checkComboBox);
		borderPane.setCenter(selectedItems);
		primaryStage.setScene(new Scene(borderPane, 400, 300));
		primaryStage.show();
	}
}