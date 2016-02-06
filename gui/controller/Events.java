package controller;

import connection.ConnectionProperty;
import connection.ConnectionPropertyList;
import connection.DatabaseProperty;
import connection.DatabasePropertyList;
import featureExtraction.Pattern;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.cell.CheckBoxTreeCell;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import org.apache.commons.lang3.StringUtils;
import run.Setting;
import utility.Meta;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.sql.SQLException;
import java.util.*;



public class Events implements Initializable {
	
	// Global variables
	private Setting setting = new Setting("GUI", "GUI");
	private List<CheckBoxTreeItem<String>> itemListTable = new ArrayList<>();
	private List<CheckBoxTreeItem<String>> itemListColumn = new ArrayList<>();
	private List<CheckBoxTreeItem<String>> itemListPattern = new ArrayList<>();
	
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
	@FXML private Label labelPredictorCountText;
	@FXML private Label labelPredictorCount;
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
		 ConnectionProperty connectionProperty = new ConnectionProperty();
		 connectionProperty.name = "GUI";
		 connectionProperty.driver = comboBoxVendor.getValue();
		 connectionProperty.database = textDatabase.getText();
		 connectionProperty.host = textHost.getText();
		 connectionProperty.port = textPort.getText();
		 connectionProperty.username = textUsername.getText();
		 connectionProperty.password = textPassword.getText();	// WE SHOULD NOT WRITE THE PASSWORD INTO THE XML
		 	 
		 // 3) Add (replace) the new connection into the list of connections
		 connectionList.setConnectionProperties(connectionProperty);
		 
		 // 4) Write it into the XML
		 connection.ConnectionPropertyList.marshall(connectionList);
		 
		 
		 ///// Connect to the server /////
		 setting = connection.Network.openConnection(setting);
		 
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
	}

	@FXML private void inputSchemaAction() {
		
		// Store the new value
		setting.inputSchema = comboBoxInputSchema.getValue();
		
		// Target tab
		SortedSet<String> tableList = Meta.collectTables(setting, setting.database, setting.inputSchema);
		comboBoxTargetTable.getItems().clear();
		comboBoxTargetTable.getItems().addAll(tableList);
				
		// Select tab
		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<>("Tables and their columns");
		rootItem.setExpanded(true);
		itemListTable.clear();
		itemListColumn.clear();
		
		for (String table : tableList) {
			final CheckBoxTreeItem<String> itemTable = new CheckBoxTreeItem<>(table);
			itemListTable.add(itemTable);
			
			// Add columns
			List<CheckBoxTreeItem<String>> localItemListColumn = new ArrayList<>();
			SortedMap<String, Integer> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, table);
			for (String column : columnList.keySet()) {
				final CheckBoxTreeItem<String> itemColumn = new CheckBoxTreeItem<>(column);
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
		
		// Target tab
		SortedMap<String, Integer> columnListType = Meta.collectColumns(setting, setting.database, setting.inputSchema, setting.targetTable);
		Set<String> columnList = columnListType.keySet();
		comboBoxTargetColumn.getItems().clear();
		comboBoxTargetId.getItems().clear();
		comboBoxTargetTimestamp.getItems().clear();
		comboBoxTargetColumn.getItems().addAll(columnList);
		comboBoxTargetId.getItems().addAll(columnList);
		comboBoxTargetTimestamp.getItems().addAll(columnList);
	}
			
	@FXML private void runAction() {

		// 1) Read current database list
		DatabasePropertyList databaseList = connection.DatabasePropertyList.unmarshall();

		// 2) Create a new database
		DatabaseProperty databaseProperty = new DatabaseProperty();
		databaseProperty.name = "GUI";
		databaseProperty.inputSchema = comboBoxInputSchema.getValue();
		databaseProperty.outputSchema = comboBoxOutputSchema.getValue();
		databaseProperty.targetTable = comboBoxTargetTable.getValue();
		databaseProperty.targetColumn = comboBoxTargetColumn.getValue();
		databaseProperty.targetId = comboBoxTargetId.getValue();
		databaseProperty.targetDate = comboBoxTargetTimestamp.getValue();
		databaseProperty.unit = comboBoxUnit.getValue();
		databaseProperty.lag = Integer.valueOf(textLag.getText());
		databaseProperty.lead = Integer.valueOf(textLead.getText());
		databaseProperty.sampleCount = Integer.valueOf(textSampleCount.getText());
		databaseProperty.task = comboBoxTask.getValue();

		// BlackList tables
		List<String> blackListTable = new ArrayList<>();
		
		for (CheckBoxTreeItem<String> treeItem : itemListTable) {
			if (!treeItem.isSelected()) {
				blackListTable.add(treeItem.getValue());
			}
		}

		databaseProperty.blackListTable = StringUtils.join(blackListTable, ',');
		
		
		// BlackList columns
		List<String> blackListColumn = new ArrayList<>();
		
		for (CheckBoxTreeItem<String> treeItem : itemListColumn) {
			if (!treeItem.isSelected()) {
				blackListColumn.add(treeItem.getParent().getValue() + "." + treeItem.getValue());
			}
		}

		databaseProperty.blackListColumn = StringUtils.join(blackListColumn, ',');
		
		// BlackList patterns
		List<String> blackListPattern = new ArrayList<>();
		
		for (CheckBoxTreeItem<String> treeItem : itemListPattern) {
			if (!treeItem.isSelected()) {
				blackListPattern.add(treeItem.getValue());
			}
		}

		databaseProperty.blackListPattern = StringUtils.join(blackListPattern, ',');
		

		// 3) Add (replace) the new connection into the list of connections
		databaseList.setDatabaseProperties(databaseProperty);

		// 4) Write it into the XML
		connection.DatabasePropertyList.marshall(databaseList);

		// 5) Clear the log window
		textAreaConsole.clear();
		
		// Execute Predictor Factory in a thread (to keep GUI responsive)
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				String[] arguments = { "GUI", "GUI" };
				run.Launcher.main(arguments);
			}
		}, "Core of Predictor Factory");
		t1.setPriority(java.lang.Thread.MIN_PRIORITY);
		t1.start();
		
	}
	

	
	
	// The initialize() method is automatically called after the FXML file has been loaded.
	// By this time, all the FXML fields are already initialized.
	// rb localize the root object. null if the root object was not localized.
    @Override public void initialize(URL fxmlFileLocation, ResourceBundle rb) {
    	
		// Setup logging into textArea (before any attempt to log anything)
    	//PropertyConfigurator.configure("/config/log4j.properties");
		controller.TextAreaAppender textAreaHandler = new TextAreaAppender();
		textAreaHandler.setTextArea(textAreaConsole);
		
		// HIDE CURRENTLY UNCONNECTED COUNTER
		labelPredictorCountText.setVisible(false);
		labelPredictorCount.setVisible(false);
		
    	// Populate comboBoxes
		// VENDOR COMBOBOX SHOULD BE POPULATED BASED ON DRIVER.XML
        comboBoxVendor.getItems().addAll("Microsoft SQL Server", "MonetDB", "Netezza", "MySQL", "Oracle", "PostgreSQL", "SAS", "Teradata");
        comboBoxTask.getItems().addAll("classification", "regression");
        comboBoxUnit.getItems().addAll("second", "hour", "day", "month", "year");
        
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
         List<String> blackListPattern  = new ArrayList<>();	// Blacklisted patterns
      
         if (databaseProperty != null) {
         	comboBoxUnit.setValue(databaseProperty.unit);
         	textLag.setText(databaseProperty.lag.toString());
         	textLead.setText(databaseProperty.lead.toString());
         	textSampleCount.setText(databaseProperty.sampleCount.toString());
         	comboBoxTask.setValue(databaseProperty.task);
         	
         	blackListPattern = Arrays.asList(databaseProperty.blackListPattern.split(","));	
         }
        
        // Connection tab
        ValidatorText.addNumericValidation(textPort);
                
		// Pattern tab - populate list of patterns
		SortedMap<String, Pattern> patternList = utility.PatternMap.getPatternMap();	// Get patternList
		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<>("Patterns");
		rootItem.setExpanded(true);
		
		for (Pattern pattern : patternList.values()) {
			final CheckBoxTreeItem<String> itemPattern = new CheckBoxTreeItem<>(pattern.name);	// Add checkboxes
			itemListPattern.add(itemPattern);
		}
		
		rootItem.getChildren().addAll(itemListPattern);
		treeViewPattern.setRoot(rootItem);
		treeViewPattern.setCellFactory(CheckBoxTreeCell.<String>forTreeView());

		for (CheckBoxTreeItem<String> item : itemListPattern) {
			if (!blackListPattern.contains(item.getValue())) item.setSelected(true);	// Check checkboxes from the last run 
		}
	
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
		
		// Setting tab
		ValidatorText.addNumericValidation(textLag);
		ValidatorText.addNumericValidation(textLead);
		ValidatorText.addNumericValidation(textSampleCount);
		
        
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