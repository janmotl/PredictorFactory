package utility;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.ComboBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import org.controlsfx.control.CheckComboBox;

import java.util.ArrayList;
import java.util.List;

/**
 * CheckCombobox must be cleared before setting new items
 *
 * @author jan
 */
public class CheckComboBoxIssue2 extends Application {

	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage primaryStage) {
		// Prepare the checkComboBox and the comboBox
		ObservableList<String> checkComboBoxContent = FXCollections.observableArrayList();
		checkComboBoxContent.add("Test item A");
		checkComboBoxContent.add("Test item B");
		checkComboBoxContent.add("Test item C");
		checkComboBoxContent.add("Test item D");
		CheckComboBox<String> checkComboBox = new CheckComboBox<>(checkComboBoxContent);

		ObservableList<String> comboBoxContent = FXCollections.observableArrayList();
		comboBoxContent.add("Test item A");
		comboBoxContent.add("Test item B");
		comboBoxContent.add("Test item C");
		comboBoxContent.add("Test item D");
		ComboBox<String> comboBox = new ComboBox<>(comboBoxContent);

		// Display the checkComboBox
		VBox root = new VBox();
		root.setAlignment(Pos.CENTER);
		root.getChildren().addAll(checkComboBox, comboBox);
		Scene scene = new Scene(root, 300, 250);
		primaryStage.setTitle("CheckComboBox issue");
		primaryStage.setScene(scene);
		primaryStage.show();

		// Simulate user's behaviour
		checkComboBox.getCheckModel().check(2);
		checkComboBox.getCheckModel().check(3);
		comboBox.getSelectionModel().select(3);

		// Workaround
		//checkComboBox.getCheckModel().clearChecks();

		// Change the underlying data
		List<String> list = new ArrayList<>();
		list.add("Apple");
		list.add("Banana");
		list.add("Cherry");
		checkComboBox.getItems().setAll(list);

		List<String> list2 = new ArrayList<>();
		list2.add("Apple");
		list2.add("Banana");
		list2.add("Cherry");
		comboBox.getItems().setAll(list2);
	}

}

