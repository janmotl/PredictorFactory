/**
 * Sample Skeleton for "simple.fxml" Controller Class
 * Use copy/paste to copy paste this code into your favorite IDE
 **/

package controller;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.sql.SQLException;
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
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBoxTreeItem;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TreeItem;
import javafx.scene.control.TreeView;
import javafx.scene.control.cell.CheckBoxTreeCell;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;

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
	private List<CheckBoxTreeItem<String>> itemListPattern = new ArrayList<CheckBoxTreeItem<String>>();
	
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
	@FXML private ComboBox<String> comboBoxUnit;
	@FXML private TextField textLag;
	@FXML private TextField textLead;
	@FXML private TextField textSampleCount;
	@FXML private TextArea textAreaConsole;
	@FXML private TextArea textAreaDescription;
	@FXML private TreeView<String> treeViewSelect;
	@FXML private TreeView<String> treeViewPattern;
	
	
	// Event handlers
	@FXML private void connectAction() {
		
		 // 0) Disconnect command?
		if ("Disconnect".equals(buttonConnect.getText())) {
			try {
				setting.connection.close();
			} catch (SQLException e) {
				e.getMessage();
			}
			
			buttonConnect.setText("Connect");
			
			return;
		}
		 
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
		 
		//exceptionDialog();	SHOULD PASS THE CONNECTION ERRORS
		 
		 // Change the button text
		 if (setting.connection != null) {
			 buttonConnect.setText("Disconnect");
		 }
		 
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
		setting.unit = comboBoxUnit.getValue();
		setting.lag = Integer.valueOf(textLag.getText());
		setting.lead = Integer.valueOf(textLead.getText());
		setting.sampleCount = Integer.valueOf(textSampleCount.getText());
		setting.task = comboBoxTask.getValue();

		// BlackList tables
		List<String> blackListTable = new ArrayList<String>(); 
		
		for (CheckBoxTreeItem<String> treeItem : itemListTable) {
			if (!treeItem.isSelected()) {
				blackListTable.add(treeItem.getValue());
			}
		}

		setting.blackListTable = StringUtils.join(blackListTable, ',');
		
		
		// BlackList columns
		List<String> blackListColumn = new ArrayList<String>(); 
		
		for (CheckBoxTreeItem<String> treeItem : itemListColumn) {
			if (!treeItem.isSelected()) {
				blackListColumn.add(treeItem.getParent().getValue() + "." + treeItem.getValue());
			}
		}

		setting.blackListColumn = StringUtils.join(blackListColumn, ',');
		
		// BlackList patterns
		List<String> blackListPattern = new ArrayList<String>(); 
		
		for (CheckBoxTreeItem<String> treeItem : itemListPattern) {
			if (!treeItem.isSelected()) {
				blackListPattern.add(treeItem.getValue());
			}
		}

		setting.blackListPattern = StringUtils.join(blackListPattern, ',');
		

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
        comboBoxUnit.getItems().addAll("second", "day", "month", "year");
        
                
		// Populate the pattern tab
		SortedMap<String, Pattern> patternList = utility.PatternMap.getPatternMap();
		
		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<String>("Patterns");
		rootItem.setExpanded(true);
		
		for (Pattern pattern : patternList.values()) {
			final CheckBoxTreeItem<String> itemPredictor = new CheckBoxTreeItem<String>(pattern.name);
			itemListPattern.add(itemPredictor);
		}
		
		rootItem.getChildren().addAll(itemListPattern);
		
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
		
		// Populate setting tab	
		textLag.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
				try {
					Integer.parseUnsignedInt(newValue); // Permit only nonnegative integers
				} catch (NumberFormatException e) {
					textLag.setText(oldValue);
				}
			}
		});
		
		textLead.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
				try {
					Integer.parseUnsignedInt(newValue); // Permit only nonnegative integers
				} catch (NumberFormatException e) {
					textLead.setText(oldValue);
				}
			}
		});
		
		textSampleCount.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
				try {
					Integer.parseUnsignedInt(newValue); // Permit only nonnegative integers
				} catch (NumberFormatException e) {
					textSampleCount.setText(oldValue);
				}
			}
		});
		
		
        // Read past setting. If no past setting is available, leave it unfilled.
		// NOTE: IF SOME ATTRIBUTE IS MISSING, IT WILL FAIL
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
        
        DatabasePropertyList databaseList = connection.DatabasePropertyList.unmarshall();
        DatabaseProperty databaseProperty = databaseList.getDatabaseProperties("GUI");
     
        if (databaseProperty != null) {
        	comboBoxUnit.setValue(databaseProperty.unit);
        	textLag.setText(databaseProperty.lag.toString());
        	textLead.setText(databaseProperty.lead.toString());
        	textSampleCount.setText(databaseProperty.sampleCount.toString());
        	comboBoxTask.setValue(databaseProperty.task);
        }
    }

    
    // Subroutine: Exception dialog
    // SHOULD BE IN A SEPARATE CLASS
    protected void exceptionDialog(Exception ex) {
	    Alert alert = new Alert(AlertType.ERROR);
	    alert.setTitle("Predictor Factory");
	    alert.setHeaderText(null);
	    alert.setContentText("Could not connect to the database");
	
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
}