package controller;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.scene.control.TextField;
import javafx.scene.effect.InnerShadow;
import javafx.scene.paint.Color;

public class ValidatorText {
	
	// Add numeric validation into a textField
	protected static void addNumericValidation(TextField textField) {
		textField.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
				if (isNumeric(newValue)) {
					textField.setStyle(null); // reset
					textField.setEffect(null); // reset
				} else {
					textField.setStyle("-fx-focus-color: #f25f29; -fx-faint-focus-color: #f25f2933;"); // Red focus
					textField.setEffect(new InnerShadow(3, new Color(1, 0, 0, 1))); // Red highlight
				}
			}
		});
	}
			
	// Subroutine: validate the text 
	private static boolean isNumeric(String text) {
		try {
            if (text == null || text.trim().equals("")) return true;
            Integer.parseUnsignedInt(text);
            return true; 
        } catch (Exception e) {
            return false;
        }
	}

}
