/**
 * Sample Skeleton for "simple.fxml" Controller Class
 * Use copy/paste to copy paste this code into your favorite IDE
 **/

package controller;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBoxTreeItem;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.control.cell.CheckBoxTreeCell;

import org.apache.commons.lang3.StringUtils;

import run.Setting;
import utility.Meta;
import connection.ConnectionProperty;
import connection.ConnectionPropertyList;
import connection.DatabaseProperty;
import connection.DatabasePropertyList;
import featureExtraction.Pattern;



public class Events implements Initializable {
	
	// Global variables
	private Setting setting = new Setting();
	private List<CheckBoxTreeItem<String>> itemListTable = new ArrayList<CheckBoxTreeItem<String>>();
	private List<CheckBoxTreeItem<String>> itemListColumn = new ArrayList<CheckBoxTreeItem<String>>();
	private List<CheckBoxTreeItem<String>> itemListPredictor = new ArrayList<CheckBoxTreeItem<String>>();
	
	// Define GUI elements. The values are automatically initialized by FXMLLoader.
    @FXML private Button buttonConnect;
    @FXML private Button buttonRun;
    @FXML private TextField textHost;
	@FXML private TextField textPort;
	@FXML private TextField textDatabase;
	@FXML private TextField textUsername;
	@FXML private TextField textPassword;
	@FXML private ComboBox<String> comboBoxVendor;
	@FXML private ComboBox<String> comboBoxInputSchema;
	@FXML private ComboBox<String> comboBoxOutputSchema;
	@FXML private ComboBox<String> comboBoxTargetTable;
	@FXML private ComboBox<String> comboBoxTargetColumn;
	@FXML private ComboBox<String> comboBoxTargetId;
	@FXML private ComboBox<String> comboBoxTargetTimestamp;
	@FXML private ComboBox<String> comboBoxTask;
	@FXML private TextArea textAreaConsole;
	@FXML private TextArea textAreaDescription;
	
	@FXML private TreeView<String> treeViewSelect;
	@FXML private TreeView<String> treeViewPattern;
	

	
	
	// Event handlers
	@FXML private void connectAction() {
		 
		 // 1) Read current connections 
		 ConnectionPropertyList connectionList = connection.ConnectionPropertyList.unmarshall();
		 
		 // 2) Create a new connection
		 ConnectionProperty property = new ConnectionProperty();
		 property.name = "GUI";
		 property.driver = comboBoxVendor.getValue();
		 property.database = textDatabase.getText();
		 property.host = textHost.getText();
		 property.port = textPort.getText();
		 property.username = textUsername.getText();
		 property.password = textPassword.getText();	// WE SHOULD NOT WRITE THE PASSWORD INTO THE XML
		 	 
		 // 3) Add (replace) the new connection into the list of connections
		 connectionList.setConnectionProperties(property);
		 
		 // 4) Write it into the XML
		 connection.ConnectionPropertyList.marshall(connectionList);
		 
		 
		 ///// Connect to the server /////
		 setting = connection.Network.openConnection(setting, "GUI", "GUI");
		 
		 
        ////// Read past setting for the database tab ///////
        DatabasePropertyList databaseList = connection.DatabasePropertyList.unmarshall();
        DatabaseProperty databaseProperty = databaseList.getDatabaseProperties("GUI");
        
        if (databaseProperty != null) {
        	comboBoxInputSchema.setValue(databaseProperty.inputSchema);
        	comboBoxOutputSchema.setValue(databaseProperty.outputSchema);
        	comboBoxTargetTable.setValue(databaseProperty.targetTable);
        	comboBoxTargetColumn.setValue(databaseProperty.targetColumn);
        	comboBoxTargetId.setValue(databaseProperty.targetId);
        	comboBoxTargetTimestamp.setValue(databaseProperty.targetDate);
        	comboBoxTask.setValue(databaseProperty.task);
        }
		 
		 ///// Populate setting in the database tab //////
		 SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
		 comboBoxInputSchema.getItems().addAll(schemaList);
		 comboBoxOutputSchema.getItems().addAll(schemaList);
		 		 
		//textHost.setEffect(new InnerShadow(3, new Color(1, 0, 0, 1))); // Highlight
	}

	@FXML private void inputSchemaAction() {
		
		// Store the new value
		setting.inputSchema = comboBoxInputSchema.getValue();
		
		// Populate setting in the target tab
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		comboBoxTargetTable.getItems().addAll(tableList);
				
		// Populate setting in the select tab
		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<String>("Tables and their columns");
		rootItem.setExpanded(true);
		
		for (String table : tableList) {
			final CheckBoxTreeItem<String> itemTable = new CheckBoxTreeItem<String>(table);
			itemListTable.add(itemTable);
			
			// Add columns
			List<CheckBoxTreeItem<String>> localItemListColumn = new ArrayList<CheckBoxTreeItem<String>>();
			SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, table);
			for (String column : columnList.keySet()) {
				final CheckBoxTreeItem<String> itemColumn = new CheckBoxTreeItem<String>(column);
				localItemListColumn.add(itemColumn);
			}
			itemTable.getChildren().addAll(localItemListColumn);
			itemListColumn.addAll(localItemListColumn);
		}
		rootItem.getChildren().addAll(itemListTable);
		
		treeViewSelect.setRoot(rootItem);
		treeViewSelect.setCellFactory(CheckBoxTreeCell.<String>forTreeView());
		
		
		
	}
	
	@FXML private void targetTableAction() {
		
		// Store the new value
		setting.targetTable = comboBoxTargetTable.getValue();
		
		// Populate other comboBoxes
		SortedMap<String, Integer> columnListType = Meta.collectColumns(setting, setting.database, setting.inputSchema, setting.targetTable);
		Set<String> columnList = columnListType.keySet();
		comboBoxTargetColumn.getItems().addAll(columnList);
		comboBoxTargetId.getItems().addAll(columnList);
		comboBoxTargetTimestamp.getItems().addAll(columnList);
	}
		
	@FXML private void runAction() {

		// 1) Read current database list
		DatabasePropertyList databaseList = connection.DatabasePropertyList.unmarshall();

		// 2) Create a new database
		DatabaseProperty setting = new DatabaseProperty();
		setting.name = "GUI";
		setting.inputSchema = comboBoxInputSchema.getValue();
		setting.outputSchema = comboBoxOutputSchema.getValue();
		setting.targetTable = comboBoxTargetTable.getValue();
		setting.targetColumn = comboBoxTargetColumn.getValue();
		setting.targetId = comboBoxTargetId.getValue();
		setting.targetDate = comboBoxTargetTimestamp.getValue();
		setting.task = comboBoxTask.getValue();

		// BlackList tables
		List<String> blackList = new ArrayList<String>(); 
		
		for (CheckBoxTreeItem<String> treeItem : itemListTable) {
			if (!treeItem.isSelected()) {
				blackList.add(treeItem.getValue());
			}
		}

		setting.blackListTable = StringUtils.join(blackList, ',');
		
		
		// BlackList columns
		List<String> blackListColumn = new ArrayList<String>(); 
		
		for (CheckBoxTreeItem<String> treeItem : itemListColumn) {
			if (!treeItem.isSelected()) {
				blackListColumn.add(treeItem.getParent().getValue() + "." + treeItem.getValue());
			}
		}

		setting.blackListColumn = StringUtils.join(blackListColumn, ',');
		

		// 3) Add (replace) the new connection into the list of connections
		databaseList.setDatabaseProperties(setting);

		// 4) Write it into the XML
		connection.DatabasePropertyList.marshall(databaseList);

		// Execute Predictor Factory in a thread (to keep GUI responsive)
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				String[] arguments = { "GUI", "GUI" };
				run.Launcher.main(arguments);
			}
		}, "Core of Predictor Factory");
		t1.start();
		
	}
	
	
	
	// The initialize() method is automatically called after the FXML file has been loaded.
	// By this time, all the FXML fields are already initialized.
    @Override public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
    	
		// Setup logging into textArea (before any attempt to log anything)
    	//PropertyConfigurator.configure("/config/log4j.properties");
		controller.TextAreaAppender textAreaHandler = new TextAreaAppender();
		textAreaHandler.setTextArea(textAreaConsole);
		
		
    	// Populate comboBoxes
		// VENDOR COMBOBOX SHOULD BE POPULATED BASED ON DRIVER.XML
        comboBoxVendor.getItems().addAll("Microsoft SQL Server", "MonetDB", "Netezza", "MySQL", "Oracle", "PostgreSQL", "SAS", "Teradata");
        comboBoxTask.getItems().addAll("classification", "regression");
        
                
		// Populate the pattern tab
		SortedMap<String, Pattern> patternList = utility.PatternMap.getPatternMap();
		
		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<String>("Patterns");
		rootItem.setExpanded(true);
		
		for (Pattern pattern : patternList.values()) {
			final CheckBoxTreeItem<String> itemPredictor = new CheckBoxTreeItem<String>(pattern.name);
			itemListPredictor.add(itemPredictor);
		}
		
		rootItem.getChildren().addAll(itemListPredictor);
		
		treeViewPattern.setRoot(rootItem);
		treeViewPattern.setCellFactory(CheckBoxTreeCell.<String>forTreeView());

	
		// Pattern tab - description field. The text changes based on the currently selected pattern.
		treeViewPattern.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<TreeItem<String>>() {

            @Override
            public void changed(ObservableValue<? extends TreeItem<String>> obs, TreeItem<String> old_val, TreeItem<String> new_val) {
                
            	if (new_val.getParent() == null) {
            		textAreaDescription.setText("");	// This is the parent node
            	} else {
            		String text = patternList.get(new_val.getValue()).description;
            		text = text.trim().replaceAll(" +", " ");	// Remove unnecessary spaces
            		text = text.replace("\t", "");	// Remove tabs
            		textAreaDescription.setText(text);
            	}
            	
            }
        });
		
		
		
		
        // Read past setting. If no past setting is available, leave it unfilled.
        ConnectionPropertyList connectionList = connection.ConnectionPropertyList.unmarshall();
        ConnectionProperty connectionProperty = connectionList.getConnectionProperties("GUI");
        
        if (connectionProperty != null) {
        	comboBoxVendor.setValue(connectionProperty.driver);
        	textDatabase.setText(connectionProperty.database);
        	textHost.setText(connectionProperty.host);
        	textPort.setText(connectionProperty.port);
        	textUsername.setText(connectionProperty.username);
        	textPassword.setText(connectionProperty.password);
        }
        
     
    }

}