package controller;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.geometry.Orientation;
import javafx.scene.control.Separator;
import javafx.scene.control.TextArea;
import javafx.scene.paint.Color;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;

import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ResourceBundle;


@SuppressWarnings("MethodMayBeStatic")
public class DevelopmentController implements Initializable {

    @FXML private TextFlow textFlow;
    @FXML private WebView webView;
    @FXML private TextArea textArea;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        Text nameText = new Text("Taken By me Long afafsd fjds asjsijasjifasdfijasdjfasdjfs \n sfaj sdfjad\n" +
                "\n\n" +
                "\n" +
                "@melkhaldi is exactly correct why this is not supported: the visual details (such as scroll position), are handled by a skin, which a user could change for their own custom skin, and the text area does not know how to work with that skin. This has been a problem often enough that I'm leaning towards moving all the functionality from the Skin to the Control itself and not use Skin at all. See also this discussion at openjfx-dev (no response from an Oracle employee yet).\n" +
                "\n" +
                "Putting StyledTextArea in a ScrollPane does not work, because StyledTextArea does not return meaningful preferred size. This is because to calculate the total size, it would have to render all the content, but StyledTextArea only renders what is necessary (currently in the viewport). Scrollbar lengths and positions are then just an estimate based on the sizes of already encountered paragraphs. For fixed sized font and no line wrapping, the height estimate happens to be precise, though.\n" +
                "\n" +
                "Does the suggested workaround of repositioning the caret work for you?\n" +
                "For this project, right now I use a TextFlow to insert decorated fragments of text.\n" +
                "\n" +
                "When a parsing node is selected, I change the text content into the TextFlow and set the relative vertical scrollbar position so that the beginning of the match, or failure, is visible.\n" +
                "\n" +
                "But I can't do that with RichTextFX as far as I can see.\n" +
                "\n" +
                "The problem is that in order to better highlight the match, I change the text entirely. I collect the fragments, build the new string and .clear() then .appendText(), this works, I get the new text; and I can style it correctly.\n" +
                "\n" +
                "But the scrollbar always jump to the bottom and I can't get it to position where I want; moreover, if I try and insert the widget into a ScrollPane (which is what I do with a TextFlow, and which is how I can position the scrollbar), the widget doesn't size correctly...\n" +
                "\n" +
                "So, do I have to use a ScrollPane anyway? Or how do I position the scrollbar to where I want?\n ois jsd jdfasd asdf asd fasdfkjas  fb ahdf as asasd asdhuf afdsaiasfasi");
        nameText.setFill(Color.CRIMSON);
        textFlow.getChildren().add(nameText);
        textFlow.getChildren().add(new Text(System.lineSeparator()));

        Text takenOn = new Text("Taken On: " + DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT).format(LocalDateTime.now()));
        takenOn.setFill(Color.CRIMSON);
        textFlow.getChildren().add(takenOn);
        textFlow.getChildren().add(new Text(System.lineSeparator()));

        textFlow.getChildren().add(new Text("this is a note"));
        textFlow.getChildren().add(new Text(System.lineSeparator()));

        // Separator
        final Separator separator = new Separator(Orientation.HORIZONTAL);
        separator.prefWidthProperty().bind(textFlow.widthProperty());
//      separator.setStyle("-fx-background-color: red;");
        textFlow.getChildren().add(separator);

        textFlow.getChildren().add(new Text(System.lineSeparator()));
        textFlow.getChildren().add(new Text("this is another note"));


        // Webview
        WebEngine engine = webView.getEngine();
        engine.setUserStyleSheetLocation("data:,body {font-size: 13px; font-family: system;}");
        engine.loadContent("<h3>Description</h3>" +
                "Apply aggregation function on numerical attributes. Following aggregate functions were selected for being supported in many databases: avg, min, max and stddev_samp." +
                "<h3>Example</h3>" +
                "Count of attended events.");



    }

}
