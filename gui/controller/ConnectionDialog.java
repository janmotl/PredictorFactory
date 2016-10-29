package controller;

import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.util.Pair;

import java.io.PrintWriter;
import java.io.StringWriter;


public class ConnectionDialog {

    // Show a dialog with an error
    public static void exceptionDialog(Exception ex) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Predictor Factory");
        alert.setHeaderText(null);
        alert.setContentText(ex.getMessage());

        // Create expandable Exception.
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        String exceptionText = sw.toString();

        Label label = new Label("The exception stacktrace was:");

        TextArea textArea = new TextArea(exceptionText);
        textArea.setEditable(false);
        textArea.setWrapText(true);

        textArea.setMaxWidth(Double.MAX_VALUE);
        textArea.setMaxHeight(Double.MAX_VALUE);
        GridPane.setVgrow(textArea, Priority.ALWAYS);
        GridPane.setHgrow(textArea, Priority.ALWAYS);

        GridPane expContent = new GridPane();
        expContent.setMaxWidth(Double.MAX_VALUE);
        expContent.add(label, 0, 0);
        expContent.add(textArea, 0, 1);

        // Set expandable Exception into the dialog pane.
        alert.getDialogPane().setExpandableContent(expContent);

        alert.showAndWait();
    }

    // Show a dialog with a connection progress bar and a cancel button
    public static Dialog progressDialog() {
        Dialog<Pair<String, String>> dialog = new Dialog<>();
        dialog.setTitle("Connecting to the server...");

        // Add indeterminate progress bar
        ProgressBar pb = new ProgressBar();
        pb.setPrefWidth(250);
        pb.setPadding(new Insets(12, 10, 0, 10));
        dialog.getDialogPane().setContent(pb);

        // Add cancel button
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CANCEL);

        // Show the dialog (and let the code continue)
        dialog.show();

        // Return the handler
        return dialog;
    }

}
