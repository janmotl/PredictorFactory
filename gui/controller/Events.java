package controller;

import connection.*;
import extraction.Pattern;
import javafx.concurrent.Service;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.*;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.CheckBoxTreeCell;
import javafx.scene.text.Text;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import meta.Column;
import meta.Table;
import org.apache.commons.lang3.StringUtils;
import org.controlsfx.control.CheckComboBox;
import run.Launcher;
import run.Setting;
import utility.*;

import java.awt.*;
import java.awt.Desktop.Action;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.List;

import static utility.FormatSQL.formatSQL;
import static utility.ParseInteger.parseInteger;
import static utility.SystemQualityControl.getPFVersion;
import static utility.TextToHTML.textToHTML;


public class Events implements Initializable {

	// Global variables
    private Setting setting = new Setting("GUI", "GUI");
    private List<CheckBoxTreeItem<String>> itemListTable = new ArrayList<>();
    private List<CheckBoxTreeItem<String>> itemListColumn = new ArrayList<>();
    private List<CheckBoxTreeItem<String>> itemListPattern = new ArrayList<>();
    private Events.RunService runService = new Events.RunService();

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
    @FXML private CheckComboBox<String> comboBoxTargetColumn;
    @FXML private CheckComboBox<String> comboBoxTargetId;
    @FXML private ComboBox<String> comboBoxTargetTimestamp;
    @FXML private ComboBox<String> comboBoxTask;
    @FXML private ComboBox<String> comboBoxUnit;
    @FXML private CheckBox checkBoxUseId;
	@FXML private CheckBox checkBoxUseTwoStages;
    @FXML private Label labelPredictorCountText;
    @FXML private Label labelPredictorCount;
    @FXML private ProgressIndicator progressWhirl;
    @FXML private TextField textLag;
    @FXML private TextField textLead;
    @FXML private TextField textSampleCount;
    @FXML private TextField textPredictorMax;
	@FXML private TextField textSecondMax;
    @FXML private TextArea textAreaConsole;
    @FXML private WebView webView;
    @FXML private TreeView<String> treeViewSelect;
    @FXML private TreeView<String> treeViewPattern;
    @FXML private TabPane tabPane;
	@FXML private Tab tabConnect;
	@FXML private Tab tabRun;
    @FXML private Text textVersion;

    // Event handlers
    @FXML private void connectAction() {

        // Close the connection, if appropriate
        if ("Disconnect".equals(buttonConnect.getText())) {
            Network.closeConnection(setting);
            buttonConnect.setText("Connect");
            textPredictorMax.setPromptText("database bounded");
            return;
        }

        // 1) Read current connections from the XML
        ConnectionPropertyList connectionList = ConnectionPropertyList.unmarshall();

        // 2) Create a new connection from the GUI
        ConnectionProperty connectionProperty = new ConnectionProperty();
        connectionProperty.name = "GUI";
        connectionProperty.driver = comboBoxVendor.getValue();
        connectionProperty.database = textDatabase.getText();
        connectionProperty.host = textHost.getText();
        connectionProperty.port = textPort.getText();
        connectionProperty.username = textUsername.getText();
        connectionProperty.password = textPassword.getText();   // WE SHOULD NOT WRITE THE PASSWORD INTO THE XML. PASS IT AS A PARAMETER TO Launcher?

        // 3) Put the new connection into the list of connections
        connectionList.setConnectionProperties(connectionProperty);

        // 4) Write the list into the XML
        ConnectionPropertyList.marshall(connectionList);

        // 5) Create a setting from the XMLs
        setting = new Setting("GUI", "GUI");

        // 6) Asynchronously connect to the database and collect metadata (tables and columns)
        getMetaData(setting);
    }

    @FXML private void inputSchemaAction() {

        // Initialization
        SortedMap<String, Table> tableMap;      // From metadata
        SortedMap<String, Column> columnMap;    // From metadata
        Map<String, SortedMap<String, Column>> tableColumnMap = new HashMap<>();    // Cached results

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
            CheckBoxTreeItem<String> itemTable = new CheckBoxTreeItem<>(table);
            itemListTable.add(itemTable);

            // Add columns
            List<CheckBoxTreeItem<String>> localItemListColumn = new ArrayList<>();
            columnMap = Meta.collectColumns(setting, setting.database, setting.inputSchema, table);
            tableColumnMap.put(table, columnMap);
            for (String column : columnMap.keySet()) {
                CheckBoxTreeItem<String> itemColumn = new CheckBoxTreeItem<>(column);
                localItemListColumn.add(itemColumn);
            }
            itemTable.getChildren().setAll(localItemListColumn);
            itemListColumn.addAll(localItemListColumn);
        }

        rootItem.getChildren().setAll(itemListTable);
        treeViewSelect.setRoot(rootItem);
        treeViewSelect.setCellFactory(CheckBoxTreeCell.forTreeView());

        setTableColumn();     // Check checkboxes based on XML

    }

    @FXML private void targetTableAction() {

        // Store the new value
        setting.targetTable = comboBoxTargetTable.getValue();

        // Target tab
        SortedMap<String, Column> columnMap = Meta.collectColumns(setting, setting.database, setting.inputSchema, setting.targetTable);
	    comboBoxTargetColumn.getItems().setAll(columnMap.keySet());
        comboBoxTargetId.getItems().setAll(columnMap.keySet());
	    List<String> temporalColumns = new ArrayList<>();
	    temporalColumns.add("");    // Permits "no temporal constraint"
	    for (Map.Entry<String, Column> entry : columnMap.entrySet()) {
		    int dataType = entry.getValue().dataType;
		    if (dataType == 91 || dataType == 92 || dataType == 93 || dataType == 2013 || dataType == 2014) temporalColumns.add(entry.getKey());
	    }
        comboBoxTargetTimestamp.getItems().setAll(temporalColumns);
    }

    @FXML private void runAction() {

        // Terminate the current execution, if appropriate
        if ("Stop".equals(buttonRun.getText())) {
            runService.cancel();
            return;
        }

        // 1) Read current database list
        DatabasePropertyList databaseList = DatabasePropertyList.unmarshall();

        // 2) Create a new database
        DatabaseProperty databaseProperty = new DatabaseProperty();
        databaseProperty.name = "GUI";
        databaseProperty.inputSchema = comboBoxInputSchema.getValue();
        databaseProperty.outputSchema = comboBoxOutputSchema.getValue();
        databaseProperty.targetTable = comboBoxTargetTable.getValue();
        databaseProperty.targetColumn = String.join(",", comboBoxTargetColumn.getCheckModel().getCheckedItems());
        databaseProperty.targetId = String.join(",", comboBoxTargetId.getCheckModel().getCheckedItems());
        databaseProperty.targetDate = comboBoxTargetTimestamp.getValue();
        databaseProperty.unit = comboBoxUnit.getValue();
        databaseProperty.lag = parseInteger(textLag.getText());
        databaseProperty.lead = parseInteger(textLead.getText());
        databaseProperty.task = comboBoxTask.getValue();
        databaseProperty.useIdAttributes = checkBoxUseId.isSelected();
        databaseProperty.useTwoStages = checkBoxUseTwoStages.isSelected();
        databaseProperty.sampleCount = parseInteger(textSampleCount.getText());
        databaseProperty.predictorMax = parseInteger(textPredictorMax.getText());
        databaseProperty.secondMax = parseInteger(textSecondMax.getText());

	    // If the target timestamp is empty, replace it with null
	    if ("".equals(databaseProperty.targetDate)) databaseProperty.targetDate = null;

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
            if (!treeItem.isSelected() && parent.isIndeterminate()) {   // If table is indeterminate and column is disabled
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
        DatabasePropertyList.marshall(databaseList);

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

    @FXML private void sendEmail() {
        Desktop desktop;

        if (Desktop.isDesktopSupported() && (desktop = Desktop.getDesktop()).isSupported(Action.MAIL)) {
            try {
                URI uri = new URI("mailto:jan.motl@fit.cvut.cz");
                desktop.mail(uri);
            } catch (IOException | URISyntaxException e) {
                e.printStackTrace();
            }
        }
    }

    @FXML private void openHomepage() {
        Desktop desktop;

        if (Desktop.isDesktopSupported() && (desktop = Desktop.getDesktop()).isSupported(Action.BROWSE)) {
            try {
                URI uri = new URI("http://predictorfactory.com/");
                desktop.browse(uri);
            } catch (IOException | URISyntaxException e) {
                e.printStackTrace();
            }
        }

    }

    // The initialize() method is automatically called after the FXML file has been loaded.
    // By this time, all the FXML fields are already initialized.
    // rb localize the root object. null if the root object was not localized.
    @Override public void initialize(URL fxmlFileLocation, ResourceBundle rb) {

        // Setup logging into textArea (before any attempt to log anything)
        TextAreaAppender textAreaAppender = new TextAreaAppender();
        textAreaAppender.setTextArea(textAreaConsole);

        // Hide progress indicators as nothing is running so far
        labelPredictorCountText.setVisible(false);
        labelPredictorCount.setVisible(false);
        progressWhirl.setVisible(false);

        // Populate comboBoxes
        // VENDOR COMBOBOX SHOULD BE POPULATED BASED ON DRIVER.XML
        comboBoxVendor.getItems().addAll("Microsoft SQL Server", "MySQL", "Oracle", "PostgreSQL", "SAS");
        comboBoxTask.getItems().addAll("classification", "regression");
        comboBoxUnit.getItems().addAll("second", "hour", "day", "month", "year");

        // Read past setting. If no past setting is available, leave it unfilled.
        // NOTE: IF SOME ATTRIBUTE IS MISSING, IT WILL FAIL -> just use try-catch
         ConnectionPropertyList connectionList = ConnectionPropertyList.unmarshall();
         ConnectionProperty connectionProperty = connectionList.getConnectionProperties("GUI");

         if (connectionProperty != null) {
            comboBoxVendor.setValue(connectionProperty.driver);
            textDatabase.setText(connectionProperty.database);
            textHost.setText(connectionProperty.host);
            textPort.setText(connectionProperty.port);
            textUsername.setText(connectionProperty.username);
            textPassword.setText(connectionProperty.password);
         }

         DatabasePropertyList databaseList = DatabasePropertyList.unmarshall();
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
	     try {textSecondMax.setText(databaseProperty.secondMax.toString());} catch (NullPointerException ignored) {}
         try {checkBoxUseId.setSelected(databaseProperty.useIdAttributes);} catch (NullPointerException ignored) {}
	     try {checkBoxUseTwoStages.setSelected(databaseProperty.useTwoStages);} catch (NullPointerException ignored) {}
         try {blackListPattern = TextParser.string2list(databaseProperty.blackListPattern);} catch (NullPointerException ignored) {}
         try {whiteListPattern = TextParser.string2list(databaseProperty.whiteListPattern);} catch (NullPointerException ignored) {}


        // Add ability to select an item in a combobox with a key stroke.
	    // Note: It is too tough to add it for ComboBoxMulti (comboBoxTargetColumn, comboBoxTargetId).
        PrefixSelectionCustomizer.customize(comboBoxInputSchema);
        PrefixSelectionCustomizer.customize(comboBoxOutputSchema);
        PrefixSelectionCustomizer.customize(comboBoxTargetTable);
        PrefixSelectionCustomizer.customize(comboBoxTargetTimestamp);
        PrefixSelectionCustomizer.customize(comboBoxTask);
        PrefixSelectionCustomizer.customize(comboBoxUnit);
        PrefixSelectionCustomizer.customize(comboBoxVendor);

        // Connection tab
        ValidatorText.addNumericValidation(textPort, Integer.MAX_VALUE);
        buttonConnect.defaultButtonProperty().bind(tabConnect.selectedProperty());

        // Pattern tab - populate list of patterns
        SortedMap<String, Pattern> patternMap = PatternMap.getPatternMap(); // Get patternMap
        CheckBoxTreeItem<String> rootItem = new CheckBoxTreeItem<>("Patterns");
        rootItem.setExpanded(true);

        for (Pattern pattern : patternMap.values()) {
            CheckBoxTreeItem<String> itemPattern = new CheckBoxTreeItem<>(pattern.name);  // Add checkboxes
            itemListPattern.add(itemPattern);
        }

        rootItem.getChildren().addAll(itemListPattern);
        treeViewPattern.setRoot(rootItem);
        treeViewPattern.setCellFactory(CheckBoxTreeCell.forTreeView());

        SortedMap selectedPattern = BlackWhiteList.filter(patternMap, blackListPattern, whiteListPattern); // White/black list

        for (CheckBoxTreeItem<String> item : itemListPattern) {
            if (selectedPattern.containsKey(item.getValue())) item.setSelected(true);   // Check checkboxes from the last run
        }

        // Pattern tab - description field. The text changes based on the currently selected pattern.
        treeViewPattern.getSelectionModel().selectedItemProperty().addListener((obs, oldValue, newValue) -> {

            WebEngine engine = webView.getEngine();
            engine.setUserStyleSheetLocation("data:,body {font-size: 13px;  font-family: system;  }"); // Set system look-and-feel

            if (newValue.getParent() == null) {
                engine.loadContent("Select patterns to use.");
            } else {
                String description = patternMap.get(newValue.getValue()).description;
                String example = patternMap.get(newValue.getValue()).example;
                    patternMap.get(newValue.getValue()).initialize(setting);
                String sql =  patternMap.get(newValue.getValue()).dialectCode;
                description = textToHTML(description);
                example = textToHTML(example);
                sql = formatSQL(sql);
                engine.loadContent("<h3>Description</h3>" + description + "<h3>Example</h3>" + example + "<h3>SQL</h3>" + sql);
            }

        });

        // Setting tab
        ValidatorText.addNumericValidation(textLag);
        ValidatorText.addNumericValidation(textLead);
        ValidatorText.addNumericValidation(textSampleCount);
        ValidatorText.addNumericValidation(textPredictorMax, setting.predictorMaxTheory);
	    ValidatorText.addNumericValidation(textSecondMax, setting.secondMax);

	    // Run tab
	    buttonRun.defaultButtonProperty().bind(tabRun.selectedProperty());

        // About tab
        textVersion.setText("Version " + getPFVersion());
    }



    // Set checkboxes in select tab based on the XML
    private void setTableColumn() {
        // Read from XML
        DatabasePropertyList databaseList = DatabasePropertyList.unmarshall();
        DatabaseProperty databaseProperty = databaseList.getDatabaseProperties("GUI");

        // Parse from XML (the data are stored in SQL-like syntax, not in XML-like syntax)
        List<String> whiteListTable = TextParser.string2list(databaseProperty.whiteListTable); // Parsed values
        List<String> blackListTable = TextParser.string2list(databaseProperty.blackListTable); // Parsed values
        Map<String,List<String>> whiteMapColumn = TextParser.list2map(TextParser.string2list(databaseProperty.whiteListColumn)); // Parsed values
        Map<String,List<String>> blackMapColumn = TextParser.list2map(TextParser.string2list(databaseProperty.blackListColumn)); // Parsed values


        // The logic (a hierarchical extension of BlackWhiteList) is following:
        //  1) whiteTable, whiteColumn -> check
        //  2) if (whiteTable.isEmpty() && whiteColumn.isEmpty()) -> check all
        //  3) blackTable, blackColumn -> uncheck


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
                column.setSelected(true);   // Check the column
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
                column.setSelected(false);  // Uncheck the column
            }
        }

    }

    // Get connection and metadata without blocking the GUI
    private void getMetaData(Setting setting) {

        // Show progress dialog
        Dialog dialog = ConnectionDialog.progressDialog();

        // Start terminable connection attempt
        Events.ConnectionService task = new Events.ConnectionService();
        task.start();

        // If connection succeeded...
        task.setOnSucceeded(event -> {
            dialog.close();
            buttonConnect.setText("Disconnect");
            tabPane.getSelectionModel().selectNext();
            this.setting = task.getValue();     // Store the setting with the open connection

            // Populate the database tab (based on the database content)
            SortedSet<String> schemaList = Meta.collectSchemas(setting, setting.database);
            comboBoxInputSchema.getItems().setAll(schemaList);
            comboBoxOutputSchema.getItems().setAll(schemaList);

            // Populate the database tab (based on the past setting)
            comboBoxInputSchema.setValue(setting.inputSchema);
            comboBoxOutputSchema.setValue(setting.outputSchema);
            comboBoxTargetTable.setValue(setting.targetTable);
	        for (String targetColumn : setting.targetColumnList) {
		        comboBoxTargetColumn.getCheckModel().check(targetColumn);
	        }
	        for (String targetId : setting.targetIdList) {
		        comboBoxTargetId.getCheckModel().check(targetId);
	        }
            comboBoxTargetTimestamp.setValue(setting.targetDate);
            comboBoxTask.setValue(setting.task);

            // Update the text in the setting tab
            textPredictorMax.setPromptText("database limit ≈ " + setting.predictorMaxTheory);
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
                    Launcher.main(arguments);
                    //FakeLogger.fakeCalculation();     // For phony runtime experience
                    return null;
                }
            };
        }
    }

}