package utility;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import org.controlsfx.control.CheckComboBox;

/**
 * Tooltips beneath CheckComboBox are being triggered
 *
 * How to reproduce: Open the popup of the CheckComboBox, notice how the popups
 * are **not** being triggered - correct behaviour.
 * Now select one of the sample-items. Now move the mouse pointer to a position
 * where one of the underlying controls (TextField or Button) reside, and the
 * respective Tooltip appears erroneously.
 *
 * @author arjan
 */
public class CheckComboBoxIssue extends Application {

    @Override
    public void start(Stage primaryStage) {

        ObservableList<String> checkComboBoxContent = FXCollections.observableArrayList();
        checkComboBoxContent.add("Test item A");
        checkComboBoxContent.add("Test item B");
        checkComboBoxContent.add("Test item C");
        checkComboBoxContent.add("Test item D");
        checkComboBoxContent.add("Test item E");

        CheckComboBox checkComboBox = new CheckComboBox(checkComboBoxContent);
	    checkComboBox.setTooltip(new Tooltip("This tooltip should be visible but is not!"));

        Button button = new Button();
        button.setText("Test Button");
        button.setTooltip(new Tooltip("This is the tooltip of the Test Button\nIt does NOT belong to the CheckComboBox!"));

        VBox root = new VBox();
        root.setAlignment(Pos.CENTER);
        root.getChildren().addAll(checkComboBox, button);

        Scene scene = new Scene(root, 300, 250);

        primaryStage.setTitle("CheckComboBox issue");
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        launch(args);
    }

}

