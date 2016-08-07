package controller;

import connection.*;
import featureExtraction.Pattern;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.concurrent.Service;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.cell.CheckBoxTreeCell;
import metaInformation.Column;
import metaInformation.Table;
import org.apache.commons.lang3.StringUtils;
import run.Setting;
import utility.*;

import java.net.URL;
import java.util.*;

import static utility.ParseInteger.parseInteger;


public class Events implements Initializable {
	
	// Global variables
	private Setting setting = new Setting("GUI", "GUI");
	private List<CheckBoxTreeItem<String>> itemListTable = new ArrayList<>();
	private List<CheckBoxTreeItem<String>> itemListColumn = new ArrayList<>();
	private List<CheckBoxTreeItem<String>> itemListPattern = new ArrayList<>();
	private RunService runService = new RunService();
	
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
	@FXML private CheckBox checkBoxUseId;
	@FXML private Label labelPredictorCountText;
	@FXML private Label labelPredictorCount;
	@FXML private ProgressIndicator progressWhirl;
	@FXML private TextField textLag;
	@FXML private TextField textLead;
	@FXML private TextField textSampleCount;
	@FXML private TextField textPredictorMax;
	@FXML private TextArea textAreaConsole;
	@FXML private TextArea textAreaDescription;
	@FXML private TreeView<String> treeViewSelect;
	@FXML private TreeView<String> treeViewPattern;
	@FXML private TabPane tabPane;

	
	// Event handlers
	@FXML private void connectAction() {

		// Close the connection, if appropriate
		if (buttonConnect.getText().equals("Disconnect")) {
			Network.closeConnection(setting);
			buttonConnect.setText("Connect");
			textPredictorMax.setPromptText("database bounded");
			return;
		}

		// 1) Read current connections from the XML
		ConnectionPropertyList connectionList = connection.ConnectionPropertyList.unmarshall();
		 
		// 2) Create a new connection from the GUI
		ConnectionProperty connectionProperty = new ConnectionProperty();
		connectionProperty.name = "GUI";
		connectionProperty.driver = comboBoxVendor.getValue();
		connectionProperty.database = textDatabase.getText();
		connectionProperty.host = textHost.getText();
		connectionProperty.port = textPort.getText();
		connectionProperty.username = textUsername.getText();
		connectionProperty.password = textPassword.getText();	// WE SHOULD NOT WRITE THE PASSWORD INTO THE XML. PASS IT AS A PARAMETER TO Launcher?
		 	 
		// 3) Put the new connection into the list of connections
		connectionList.setConnectionProperties(connectionProperty);
		 
		// 4) Write the list into the XML
		connection.ConnectionPropertyList.marshall(connectionList);

		// 5) Create a setting from the XMLs
		setting = new Setting("GUI", "GUI");
		 
		// 6) Asynchronously connect to the database and collect metadata (tables and columns)
		getMetaData(setting);
	}

	@FXML private void inputSchemaAction() {

		// Initialization
		SortedMap<String, Table> tableMap;		// From metadata
		SortedMap<String, Column> columnMap;	// From metadata
		Map<String, SortedMap<String, Column>> tableColumnMap = new HashMap<>();	// Cached results
		
		// Store the new value
		setting.inputSchema = comboBoxInputSchema.getValue();
		
		// Target tab
		tableMap = Meta.collectTables(setting, setting.database, setting.inputSchema);
		comboBoxTargetTable.getItems().setAll(tableMap.keySet());
				
		// Select tab
		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<>("Tables and their columns");
		rootItem.setExpanded(true);
		itemListTable.clear();
		itemListColumn.clear();
		
		for (String table : tableMap.keySet()) {
			final CheckBoxTreeItem<String> itemTable = new CheckBoxTreeItem<>(table);
			itemListTable.add(itemTable);
			
			// Add columns
			List<CheckBoxTreeItem<String>> localItemListColumn = new ArrayList<>();
			columnMap = Meta.collectColumns(setting, setting.database, setting.inputSchema, table);
			tableColumnMap.put(table, columnMap);
			for (String column : columnMap.keySet()) {
				final CheckBoxTreeItem<String> itemColumn = new CheckBoxTreeItem<>(column);
				localItemListColumn.add(itemColumn);
			}
			itemTable.getChildren().setAll(localItemListColumn);
			itemListColumn.addAll(localItemListColumn);
		}

		rootItem.getChildren().setAll(itemListTable);
		treeViewSelect.setRoot(rootItem);
		treeViewSelect.setCellFactory(CheckBoxTreeCell.forTreeView());

		checkTableColumn(tableMap, tableColumnMap); 	// Check checkboxes based on XML

	}
	
	@FXML private void targetTableAction() {
		
		// Store the new value
		setting.targetTable = comboBoxTargetTable.getValue();
		
		// Target tab
		Set<String> columnList = Meta.collectColumns(setting, setting.database, setting.inputSchema, setting.targetTable).keySet();
		comboBoxTargetColumn.getItems().setAll(columnList);
		comboBoxTargetId.getItems().setAll(columnList);
		comboBoxTargetTimestamp.getItems().setAll(columnList);
	}
			
	@FXML private void runAction() {

		// Terminate the current execution, if appropriate
		if (buttonRun.getText().equals("Stop")) {
			runService.cancel();
			return;
		}

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
		databaseProperty.lag = parseInteger(textLag.getText());
		databaseProperty.lead = parseInteger(textLead.getText());
		databaseProperty.task = comboBoxTask.getValue();
		databaseProperty.useIdAttributes = checkBoxUseId.isSelected();
		databaseProperty.sampleCount = parseInteger(textSampleCount.getText());
		databaseProperty.predictorMax = parseInteger(textPredictorMax.getText());

		// BlackList tables
		List<String> blackListTable = new ArrayList<>();
		
		for (CheckBoxTreeItem<String> treeItem : itemListTable) {
			if (!treeItem.isSelected() && !treeItem.isIndeterminate()) {  // Only if the whole table is disabled
				blackListTable.add(treeItem.getValue());
			}
		}

		databaseProperty.blackListTable = StringUtils.join(blackListTable, ',');
		if (databaseProperty.blackListTable.isEmpty()) {
			databaseProperty.blackListTable = null; // If empty, do not write the attribute into the XML
		}
		
		// BlackList columns
		List<String> blackListColumn = new ArrayList<>();
		
		for (CheckBoxTreeItem<String> treeItem : itemListColumn) {
			CheckBoxTreeItem parent = (CheckBoxTreeItem) treeItem.getParent();
			if (!treeItem.isSelected() && parent.isIndeterminate()) {	// If table is indeterminate and column is disabled
				blackListColumn.add(treeItem.getParent().getValue() + "." + treeItem.getValue());
			}
		}

		databaseProperty.blackListColumn = StringUtils.join(blackListColumn, ',');
		if (databaseProperty.blackListColumn.isEmpty()) {
			databaseProperty.blackListColumn = null; // If empty, do not write the attribute into the XML
		}
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

		// 6) Start the odyssey
		runService.restart();

		// Provide feedback to the user to wait
		runService.setOnRunning(event -> {
			buttonRun.setText("Stop");
			progressWhirl.setVisible(true);
		});

		// There are three plausible exit states. Cover them all.
		runService.setOnCancelled(event -> {
			buttonRun.setText("Start");
			progressWhirl.setVisible(false);
		});
		runService.setOnFailed(event -> {
			buttonRun.setText("Start");
			progressWhirl.setVisible(false);
		});
		runService.setOnSucceeded(event -> {
			buttonRun.setText("Start");
			progressWhirl.setVisible(false);
		});


	}


	// The initialize() method is automatically called after the FXML file has been loaded.
	// By this time, all the FXML fields are already initialized.
	// rb localize the root object. null if the root object was not localized.
    @Override public void initialize(URL fxmlFileLocation, ResourceBundle rb) {
    	
		// Setup logging into textArea (before any attempt to log anything)
    	//PropertyConfigurator.configure("/config/log4j.properties");
		TextAreaAppender textAreaHandler = new TextAreaAppender();
		textAreaHandler.setTextArea(textAreaConsole);
		
		// Hide progress indicators as nothing is running so far
		labelPredictorCountText.setVisible(false);
		labelPredictorCount.setVisible(false);
		progressWhirl.setVisible(false);
		
    	// Populate comboBoxes
		// VENDOR COMBOBOX SHOULD BE POPULATED BASED ON DRIVER.XML
        comboBoxVendor.getItems().addAll("Microsoft SQL Server", "MonetDB", "Netezza", "MySQL", "Oracle", "PostgreSQL", "SAS", "Teradata");
        comboBoxTask.getItems().addAll("classification", "regression");
        comboBoxUnit.getItems().addAll("second", "hour", "day", "month", "year");
        
        // Read past setting. If no past setting is available, leave it unfilled.
 		// NOTE: IF SOME ATTRIBUTE IS MISSING, IT WILL FAIL -> just use try-catch
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
         List<String> blackListPattern  = new ArrayList<>();
		 List<String> whiteListPattern  = new ArrayList<>();
      
         // Database properties
		 try {comboBoxUnit.setValue(databaseProperty.unit);} catch (NullPointerException ignored) {}
		 try {textLag.setText(databaseProperty.lag.toString());} catch (NullPointerException ignored) {}
		 try {textLead.setText(databaseProperty.lead.toString());} catch (NullPointerException ignored) {}
		 try {textSampleCount.setText(databaseProperty.sampleCount.toString());} catch (NullPointerException ignored) {}
		 try {comboBoxTask.setValue(databaseProperty.task);} catch (NullPointerException ignored) {}
		 try {textPredictorMax.setText(databaseProperty.predictorMax.toString());} catch (NullPointerException ignored) {}
		 try {checkBoxUseId.setSelected(databaseProperty.useIdAttributes);} catch (NullPointerException ignored) {}
		 try {blackListPattern = Text.string2list(databaseProperty.blackListPattern);} catch (NullPointerException ignored) {}
		 try {whiteListPattern = Text.string2list(databaseProperty.whiteListPattern);} catch (NullPointerException ignored) {}


		// Add ability to select an item in a combobox with a key stroke
		PrefixSelectionCustomizer.customize(comboBoxInputSchema);
		PrefixSelectionCustomizer.customize(comboBoxOutputSchema);
		PrefixSelectionCustomizer.customize(comboBoxTargetColumn);
		PrefixSelectionCustomizer.customize(comboBoxTargetId);
		PrefixSelectionCustomizer.customize(comboBoxTargetTable);
		PrefixSelectionCustomizer.customize(comboBoxTargetTimestamp);
		PrefixSelectionCustomizer.customize(comboBoxTask);
		PrefixSelectionCustomizer.customize(comboBoxUnit);
		PrefixSelectionCustomizer.customize(comboBoxVendor);

        // Connection tab
        ValidatorText.addNumericValidation(textPort, Integer.MAX_VALUE);
                
		// Pattern tab - populate list of patterns
		SortedMap<String, Pattern> patternMap = utility.PatternMap.getPatternMap();	// Get patternMap
		CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<>("Patterns");
		rootItem.setExpanded(true);
		
		for (Pattern pattern : patternMap.values()) {
			final CheckBoxTreeItem<String> itemPattern = new CheckBoxTreeItem<>(pattern.name);	// Add checkboxes
			itemListPattern.add(itemPattern);
		}
		
		rootItem.getChildren().addAll(itemListPattern);
		treeViewPattern.setRoot(rootItem);
		treeViewPattern.setCellFactory(CheckBoxTreeCell.forTreeView());

		SortedMap selectedPattern = BlackWhiteList.filter(patternMap, blackListPattern, whiteListPattern); // White/black list

		for (CheckBoxTreeItem<String> item : itemListPattern) {
			if (selectedPattern.containsKey(item.getValue())) item.setSelected(true);	// Check checkboxes from the last run
		}
	
		// Pattern tab - description field. The text changes based on the currently selected pattern.
		treeViewPattern.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<TreeItem<String>>() {

            @Override
            public void changed(ObservableValue<? extends TreeItem<String>> obs, TreeItem<String> old_val, TreeItem<String> new_val) {
                
            	if (new_val.getParent() == null) {
            		textAreaDescription.setText("");	// This is the parent node
            	} else {
            		String text = patternMap.get(new_val.getValue()).description;
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
		ValidatorText.addNumericValidation(textPredictorMax, setting.predictorMaxTheory);

    }



	// Set checkboxes in select tab based on the XML
	// Input (a simplified explanation):
	// 	list of tables
	//	map of {table, columnList}
	private void checkTableColumn(SortedMap<String, Table> tableMap, Map<String, SortedMap<String, Column>> tableColumnMap) {
		// Read from XML
		DatabasePropertyList databaseList = connection.DatabasePropertyList.unmarshall();
		DatabaseProperty databaseProperty = databaseList.getDatabaseProperties("GUI");

		// Parse from XML (the data are stored in SQL-like syntax, not in XML-like syntax)
		final List<String> whiteListTable = Text.string2list(databaseProperty.whiteListTable); // Parsed values
		final List<String> blackListTable = Text.string2list(databaseProperty.blackListTable); // Parsed values
		final Map<String,List<String>> whiteMapColumn = Text.list2map(Text.string2list(databaseProperty.whiteListColumn)); // Parsed values
		final Map<String,List<String>> blackMapColumn = Text.list2map(Text.string2list(databaseProperty.blackListColumn)); // Parsed values


		// The logic (a hierarchical extension of BlackWhiteList) is following:
		//	1) whiteTable, whiteColumn -> check
		//	2) if (whiteTable.isEmpty() && whiteColumn.isEmpty()) -> check all
		//	3) blackTable, blackColumn -> uncheck


		// 1) If whiteTableList contains the tableName, check it
		for (CheckBoxTreeItem<String> table : itemListTable) {
			if (whiteListTable.contains(table.getValue())) {
				table.setSelected(true);    // Check the table
			}
		}

		for (CheckBoxTreeItem<String> column : itemListColumn) {
			String tableName = column.getParent().getValue();
			List<String> whiteListColumn = whiteMapColumn.getOrDefault(tableName, new ArrayList<>());

			if (whiteListColumn.contains(column.getValue())) {
				column.setSelected(true);	// Check the column
			}
		}

		// 2) If both, whiteTableList and whiteColumnList are empty, check all
		if (whiteListTable.isEmpty() && whiteMapColumn.isEmpty()) {
			CheckBoxTreeItem root = (CheckBoxTreeItem)treeViewSelect.getRoot();
			root.setSelected(true);
		}

		// 3) If blackTableList contains the tableName, uncheck it
		for (CheckBoxTreeItem<String> table : itemListTable) {
			if (blackListTable.contains(table.getValue())) {
				table.setSelected(false);    // Uncheck the table
			}
		}

		for (CheckBoxTreeItem<String> column : itemListColumn) {
			String tableName = column.getParent().getValue();
			List<String> blackListColumn = blackMapColumn.getOrDefault(tableName, new ArrayList<>());

			if (blackListColumn.contains(column.getValue())) {
				column.setSelected(false);	// Uncheck the column
			}
		}

	}

	// Get connection and metadata without blocking the GUI
	private void getMetaData(Setting setting) {

		// Show progress dialog
		Dialog dialog = ConnectionDialog.progressDialog();

		// Start terminable connection attempt
		ConnectionService task = new ConnectionService();
		task.start();

		// If connection succeeded...
		task.setOnSucceeded(event -> {
			dialog.close();
			buttonConnect.setText("Disconnect");
			tabPane.getSelectionModel().selectNext();
			this.setting = task.getValue();		// Store the setting with the open connection

			// Populate the database tab (based on the database content)
			SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
			comboBoxInputSchema.getItems().setAll(schemaList);
			comboBoxOutputSchema.getItems().setAll(schemaList);

			// Populate the database tab (based on the past setting)
			comboBoxInputSchema.setValue(setting.inputSchema);
			comboBoxOutputSchema.setValue(setting.outputSchema);
			comboBoxTargetTable.setValue(setting.targetTable);
			comboBoxTargetColumn.setValue(setting.targetColumn);
			comboBoxTargetId.setValue(setting.targetIdList.get(0));
			comboBoxTargetTimestamp.setValue(setting.targetDate);
			comboBoxTask.setValue(setting.task);

			// Update the text in the setting tab
			textPredictorMax.setPromptText("database limit ≈" + setting.predictorMaxTheory);
		});

		// If failed...
		task.setOnFailed(event -> {
			dialog.close();

			Exception exception = (Exception) task.getException();
			ConnectionDialog.exceptionDialog(exception);
		});
	}

	// Takes a setting, makes a connection, returns the setting
	private class ConnectionService extends Service<Setting> {
		@Override
		protected Task<Setting> createTask() {
			return new Task<Setting>() {
				@Override
				protected Setting call() throws Exception {
					return  Network.openConnection(setting);
				}
			};
		}
	}

	// Execute Predictor Factory
	private class RunService extends Service<Void> {

		@Override
		protected Task<Void> createTask() {
			return new Task<Void>() {
				@Override
				protected Void call() throws Exception {
					String[] arguments = { "GUI", "GUI" };
					run.Launcher.main(arguments);
					return null;
				}
			};
		}
	}

}