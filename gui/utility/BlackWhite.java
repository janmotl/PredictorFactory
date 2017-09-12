package utility;

import javafx.scene.control.CheckBoxTreeItem;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BlackWhite {

	// NOTE: We may have to first expand the tree branches to be able to walk over them!
	public static void column(TreeView<String> treeViewSelect, Map<String, Map<String, List<String>>> whiteMapColumn, boolean value) {
		// We have to loop over the tree items because trees are not like sets - we can only iterate over them
		for (TreeItem<String> schema : treeViewSelect.getRoot().getChildren()) {
			for (TreeItem<String> table : schema.getChildren()) {
				for (TreeItem<String> column : table.getChildren()) {

					// Casts
					String schemaName = schema.getValue();
					String tableName = table.getValue();
					String columnName = column.getValue();
					CheckBoxTreeItem<String> columnItem = (CheckBoxTreeItem<String>) column;

					// Check match to the content of whiteMapColumn
					if (whiteMapColumn.containsKey(schemaName)) {
						if (whiteMapColumn.get(schemaName).containsKey(tableName)) {
							for (String whiteColumn : whiteMapColumn.get(schemaName).get(tableName)) {
								if (columnName.equals(whiteColumn)) {
									columnItem.setSelected(value);
								}
							}
						}
					}

				}
			}
		}
	}

	public static String columnOut(TreeView<String> treeViewSelect) {
		List<String> blackListColumn = new ArrayList<>();

		for (TreeItem<String> schema : treeViewSelect.getRoot().getChildren()) {
			for (TreeItem<String> table : schema.getChildren()) {
				for (TreeItem<String> column : table.getChildren()) {

					// Casts
					String schemaName = schema.getValue();
					String tableName = table.getValue();
					String columnName = column.getValue();
					CheckBoxTreeItem<String> columnItem = (CheckBoxTreeItem<String>) column;
					CheckBoxTreeItem<String> tableItem = (CheckBoxTreeItem<String>) table;

					// Check match to the content of whiteMapColumn
					if (!columnItem.isSelected() && tableItem.isIndeterminate()) {   // If table is indeterminate and column is disabled
						blackListColumn.add(schemaName + "." + tableName + "." + columnName);
					}

				}
			}
		}

		String result = String.join(",", blackListColumn);
		if (blackListColumn.isEmpty()) {
			result = null; // If empty, do not write the attribute into the XML
		}

		return result;
	}

	public static String tableOut(TreeView<String> treeViewSelect) {
		List<String> blackListTable = new ArrayList<>();

		for (TreeItem<String> schema : treeViewSelect.getRoot().getChildren()) {
			for (TreeItem<String> table : schema.getChildren()) {
					// Casts
					String schemaName = schema.getValue();
					String tableName = table.getValue();
					CheckBoxTreeItem<String> schemaItem = (CheckBoxTreeItem<String>) schema;
					CheckBoxTreeItem<String> tableItem = (CheckBoxTreeItem<String>) table;

					// Check match to the content of whiteMapColumn
					if (!tableItem.isSelected() && schemaItem.isIndeterminate()) {   // If table is indeterminate and column is disabled
						blackListTable.add(schemaName + "." + tableName);
					}
			}
		}

		String result = String.join(",", blackListTable);
		if (blackListTable.isEmpty()) {
			result = null; // If empty, do not write the attribute into the XML
		}

		return result;
	}


}
