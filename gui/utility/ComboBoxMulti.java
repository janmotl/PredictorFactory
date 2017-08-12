package utility;

import javafx.scene.control.CheckMenuItem;
import javafx.scene.control.MenuButton;
import javafx.scene.control.MenuItem;
import meta.Column;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// A version of comboBox, which permits selection of multiple items.
// Unfortunately, it has to be extended to work with FXML:
//  https://stackoverflow.com/questions/31569299/how-to-extend-custom-javafx-components-that-use-fxml
public class ComboBoxMulti extends MenuButton {
	final private List<String> selectedItems = new ArrayList<>();

	// Define the items
	public void setAll(Map<String, Column> columnMap) {
		selectedItems.clear();

		// Create items
		List<CheckMenuItem> itemList = columnMap.keySet().stream().map(t -> new CheckMenuItem(t)).collect(Collectors.toList());
		this.getItems().setAll(itemList);

		// Automatically update selectedItems and rename the ComboBoxMulti based on the selected items
		for (final CheckMenuItem item : itemList) {
			item.selectedProperty().addListener((observableValue, oldValue, newValue) -> {
				if (newValue) {
					selectedItems.add(item.getText());
				} else {
					selectedItems.remove(item.getText());
				}
				this.setText(String.join(", ", selectedItems));
			});
		}
	}

	// Check mark selected items, overwriting the previous state
	public void setValues(Collection<String> selectedItems) {
		this.selectedItems.clear();
		this.selectedItems.addAll(selectedItems);
	}

	// Returns String with comma delimited selected items
	public String getValues() {
		List<String> selected = new ArrayList<>();

		for (MenuItem menuItem : this.getItems()) {
			selected.add(menuItem.getText());
		}

		return String.join(",", selected);
	}
}
