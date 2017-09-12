package utility;

import connection.Network;
import javafx.application.Application;
import javafx.beans.InvalidationListener;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.CheckBoxTreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.control.cell.CheckBoxTreeCell;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import meta.Column;
import meta.Table;
import run.Setting;

import java.util.*;


public class Tree extends Application {

	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage primaryStage) {
		// Initialize
		TreeView<String> treeView = new TreeView<>();

		// Display the checkComboBox
		VBox root = new VBox();
		root.setAlignment(Pos.CENTER);
		root.getChildren().addAll(treeView);
		Scene scene = new Scene(root, 300, 250);
		primaryStage.setTitle("CheckComboBox issue");
		primaryStage.setScene(scene);
		primaryStage.show();

		// Connect
		utility.Logging.initialization();
		Setting setting = new Setting("PostgreSQL", "financial");
		setting = Network.openConnection(setting);

		// Metadata
		SortedSet<String> schemas = Meta.collectSchemas(setting, setting.database);
		SortedMap<String, Table> tables = Meta.collectTables(setting, setting.database, "financial");

		initialize(treeView, setting, schemas, tables, "financial");
	}

	// SchemaList should be possibly passed
	public static void initialize(TreeView<String> treeView, Setting setting, SortedSet<String> schemas, SortedMap<String, Table> tables, String targetSchema) {
		// Populate with schemas after connecting to the database
		List<CheckBoxTreeItem<String>> schemaItems = getCheckBoxTreeItems(schemas);

		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<>("Schemas, tables and their columns");
		rootItem.getChildren().setAll(schemaItems);
		rootItem.setExpanded(true);

		treeView.setRoot(rootItem);
		treeView.setCellFactory(CheckBoxTreeCell.forTreeView());

		// Populate with tables for targetSchema
		List<CheckBoxTreeItem<String>> tableItems = getCheckBoxTreeItems(tables.keySet());

		for (int schemaIndex = 0; schemaIndex < schemaItems.size(); schemaIndex++) {
			CheckBoxTreeItem<String> schemaItem = schemaItems.get(schemaIndex);
			if (schemaItem.getValue().equals(targetSchema)) {
				schemaItem.getChildren().setAll(tableItems);
				addColumnListener(schemaItem.getValue(), tableItems, setting);
				changeFocus(treeView, schemaIndex);
				schemaItem.setExpanded(true);
			} else {
				// Make the item expandable (but load the actual data only on request)
				List<CheckBoxTreeItem<String>> placeholder = new ArrayList<>();
				placeholder.add(new CheckBoxTreeItem<>(""));
				schemaItem.getChildren().setAll(placeholder);

				// Load the data only once during the expansion of the node...
				schemaItem.expandedProperty().addListener(getInvalidationListener(schemaItem, setting));

				// ...or during the selection of the checkbox.
				schemaItem.selectedProperty().addListener(getInvalidationListener(schemaItem, setting));

			}
		}
	}

	// Subroutines
	private static InvalidationListener getInvalidationListener(CheckBoxTreeItem<String> schemaItem, Setting setting) {
		return observable -> {
			if ("[TreeItem [ value:  ]]".equals(schemaItem.getChildren().toString())) {
				SortedMap<String, Table> tableMap = Meta.collectTables(setting, setting.database, schemaItem.getValue());
				List<CheckBoxTreeItem<String>> tableItems = getCheckBoxTreeItems(tableMap.keySet());
				addColumnListener(schemaItem.getValue(), tableItems, setting);
				schemaItem.getChildren().setAll(tableItems);
			}
		};
	}

	private static List<CheckBoxTreeItem<String>> getCheckBoxTreeItems(Collection<String> collection) {
		List<CheckBoxTreeItem<String>> list = new ArrayList<>();

		for (String name : collection) {
			CheckBoxTreeItem<String> item = new CheckBoxTreeItem<>(name);
			list.add(item);
		}

		return list;
	}

	private static void addColumnListener(String schemaName, List<CheckBoxTreeItem<String>> tableItems, Setting setting) {
		for (CheckBoxTreeItem<String> tableItem : tableItems) {
			// Make the item expandable (but load the actual data only on request)
			List<CheckBoxTreeItem<String>> placeholder = new ArrayList<>();
			placeholder.add(new CheckBoxTreeItem<>(""));
			tableItem.getChildren().setAll(placeholder);

			// Load the data only once during the expansion of the node...
			tableItem.expandedProperty().addListener(observable -> {
				if ("[TreeItem [ value:  ]]".equals(tableItem.getChildren().toString())) {
					SortedMap<String, Column> local = Meta.collectColumns(setting, setting.database, schemaName, tableItem.getValue());
					List<CheckBoxTreeItem<String>> localItems = getCheckBoxTreeItems(local.keySet());
					tableItem.getChildren().setAll(localItems);
				}
			});

			// ...or during the selection of the checkbox.
			tableItem.selectedProperty().addListener(observable -> {
				if ("[TreeItem [ value:  ]]".equals(tableItem.getChildren().toString())) {
					SortedMap<String, Column> local = Meta.collectColumns(setting, setting.database, schemaName, tableItem.getValue());
					List<CheckBoxTreeItem<String>> localItems = getCheckBoxTreeItems(local.keySet());
					tableItem.getChildren().setAll(localItems);
				}
			});
		}
	}

	private static void changeFocus(TreeView<String> treeView, int targetSchemaIndex) {
		try {
			treeView.scrollTo(targetSchemaIndex - 2); // One item above should be visible
			treeView.getFocusModel().focus(targetSchemaIndex + 1); // The root item is not counted --> increment by one
		} catch (RuntimeException ignored) {
			// We can get out of bounds (e.g. when the tree is empty)
		}
	}

}

