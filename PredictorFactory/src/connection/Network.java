package connection;


import java.io.Console;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.SortedSet;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import run.Setting;
import utility.Meta;



public final class Network {
	// Logging
	private static final Logger logger = Logger.getLogger(Network.class.getName());
	
	// Return the connection and database configuration
	// WOULDN'T java DataSource (javax.sql.DataSource) BE ENOUGH?
	// I WOULD HAVE TO USE JNDI OR DRIVER SPECIFIC CLASS -> LET'S KEEP THE IMPLEMENTATION SIMPLE
	// CONSIDER USING c3p0 (or HikariCP) DataSources for Recovery From Database Outages
	// SEE: http://http://www.mchange.com/projects/c3p0/
	public static Setting getConnection(Setting setting, String connectionPropertyName, String databasePropertyName) {
        
        // Load the connection configuration from a XML
		ConnectionPropertyList connectionPropertyList = ConnectionPropertyList.unmarshall();
		DriverPropertyList driverPropertyList = DriverPropertyList.unmarshall(); 
		DatabasePropertyList databasePropertyList = DatabasePropertyList.unmarshall();
        
        // Get the configuration for the specified database vendor		
        ConnectionProperty connectionProperty = connectionPropertyList.getConnectionProperties(connectionPropertyName);
        DriverProperty driverProperty = driverPropertyList.getDriverProperties(connectionProperty.driver);
        DatabaseProperty databaseProperty = databasePropertyList.getDatabaseProperties(databasePropertyName);
        
		// Build a DataSource
		// Database is required because MySQL refuses to return DatabaseMetaData 
		// within a connection, which was made without database.
        BasicDataSource ds = new BasicDataSource(); 
		ds.setDriverClassName(driverProperty.driverClass);
		ds.setUsername(connectionProperty.username);
		ds.setPassword(connectionProperty.password);
		String url = driverProperty.urlPrefix + connectionProperty.host + ":" + connectionProperty.port 
				   + driverProperty.dbNameSeparator + connectionProperty.database;
		ds.setUrl(url);
		
		// If the connectionProperty doesn't contain username and/or password, prompt the user.
		// Note: The only way how to mask the password (that I am aware of) is to ask for the password 
		// during the runtime. And when we are asking for the password, we may also ask for the username, if necessary.
		try (Scanner input = new Scanner(System.in)) {	// The issue with scanner is that once it's closed it can't be reopened.
			if (connectionProperty.username == null) {
				System.out.print("Enter your username for the database: ");
				ds.setUsername(input.nextLine());
			}
			if (connectionProperty.password == null) {
				System.out.print("Enter your password for the database: ");
				Console console = System.console();
				if (console == null) {	// In Eclipse IDE "console" doesn't work. Use regular System.in instead.
					ds.setPassword(input.nextLine());
				} else { 				// Outside Eclipse IDE passwords are masked as expected. 
					ds.setPassword(new String(console.readPassword()));
				}
			}
		}
		 
		// Connect to the server
		int indentifierLengthMax = 0;
		int columnMax = 0;
		logger.debug("Connecting to the server with the following URL: " + ds.getUrl());
		
		try {
			setting.connection = ds.getConnection();
			
			// log metadata 
			java.sql.DatabaseMetaData metaData = setting.connection.getMetaData();
			logger.debug("Database product name: " + metaData.getDatabaseProductName());
			logger.debug("Driver version: " + metaData.getDriverVersion());
			logger.debug("JDBC version: " + metaData.getJDBCMajorVersion() + "." + metaData.getJDBCMinorVersion() + " (expected at least 4.0)");
			logger.debug("Maximum number of characters for a column name: " + metaData.getMaxColumnNameLength());
			logger.debug("Maximum number of characters in a table name: " + metaData.getMaxTableNameLength());
			logger.debug("Maximum number of columns in a table: " + metaData.getMaxColumnsInTable());
			logger.debug("Supports ANSI92 Entry level SQL: " + metaData.supportsANSI92EntryLevelSQL());
			logger.debug("Identifier quote string: " + metaData.getIdentifierQuoteString());
			
			// collect metadata
			indentifierLengthMax = Math.min(metaData.getMaxColumnNameLength(), metaData.getMaxTableNameLength());
			columnMax = metaData.getMaxColumnsInTable();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// Set other settings
		setting.indentifierLengthMax = indentifierLengthMax;
		setting.columnMax = columnMax;
		if (columnMax==0) {
			setting.columnMax = Integer.MAX_VALUE;	// Databases with unlimited column count returns 0 -> use big default.
		}
			
		setting.database = connectionProperty.database;
		setting.databaseVendor = connectionProperty.driver;
		setting.quoteMarks = driverProperty.quoteMarks;
		setting.isCreateTableAsCompatible = "yes".equals(driverProperty.createTableAsCompatible);
		setting.isSchemaCompatible = "yes".equals(driverProperty.schemaCompatible);
		setting.dateAddSyntax = driverProperty.dateAddSyntax;
		setting.dateAddMonth = driverProperty.dateAddMonth;
		setting.dateDiffSyntax = driverProperty.dateDiffSyntax;
		setting.dateToNumber = driverProperty.dateToNumber;
		setting.insertTimestampSyntax = driverProperty.insertTimestampSyntax;
		setting.stdDevCommand = driverProperty.stdDevCommand;
		setting.charLengthCommand = driverProperty.charLengthCommand;
		setting.typeDecimal = driverProperty.typeDecimal;
		setting.typeInteger = driverProperty.typeInteger;
		setting.typeTimestamp = driverProperty.typeTimestamp;
		setting.typeVarchar = driverProperty.typeVarchar;
		setting.withData = "yes".equals(driverProperty.withData);
		
		setting.inputSchema = databaseProperty.inputSchema;
		setting.outputSchema = databaseProperty.outputSchema;
		setting.targetId = databaseProperty.targetId;
		setting.targetDate = databaseProperty.targetDate;
		setting.targetColumn = databaseProperty.targetColumn;
		setting.targetTable = databaseProperty.targetTable;
		setting.blackListTable = databaseProperty.blackListTable;
		setting.blackListColumn = databaseProperty.blackListColumn;
		setting.task = databaseProperty.task;
		
		
		// QC input and output schemas
		SortedSet<String> schemaSet = Meta.collectSchemas(setting, setting.database);
		if (!schemaSet.contains(setting.inputSchema)) {
			logger.warn("The input schema \"" + setting.inputSchema + "\" doesn't exist in \"" + setting.database + "\" database.");
		}
		if (!schemaSet.contains(setting.outputSchema)) {
			logger.warn("The output schema \"" + setting.outputSchema + "\" doesn't exist in \"" + setting.database + "\" database.");
		}
		
		return setting;
	}
     
	// If we are opening the connection here, we have to close the connection here.
	public static void closeConnection(Setting setting) {
		try {
			setting.connection.close();
		} catch (SQLException e) {
			logger.error("The connection to the server wasn't closed as the connection is already inactive.");
		}
	}
	
	// Execute update. Returns true if the update was successful. False otherwise.
    public static boolean executeUpdate(Connection connection, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization
		boolean isOk = false;
	
		// Query using try-with-resources
	    try (Statement stmt = connection.createStatement()) {
	    	
	    	stmt.executeUpdate(sql);
			isOk = true; 
			
			// Remove line breaks and collapse all "whitespace substrings" longer than one character.
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " "); 		
			logger.debug(sql);
		} catch (SQLException e) {
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
			logger.info(e.getMessage() + " | " + sql);
			try {
				// The isValid method is used to check if a connection is still usable after an SQL exception has been thrown. 
				// SHOULD BE DEALED AT POOL LEVEL.
				boolean isConnected = connection.isValid(1000);
				if (!isConnected) logger.warn("The connection doesn't appear to be open.");
				// CALL getConnection. RERUN THE CODE? AND RETURN NEW CONNECTION.
				// SEE: http://stackoverflow.com/questions/8345133/when-my-app-loses-connection-how-should-i-try-to-recover
			} catch (SQLException ignored) {}
		} 
	    
	    // Technical: Do not close the connection here. The connection is closed at the end of the Launcher. 
	    
	    return isOk;
    }
    
    
 	// Get list of strings
    public static List<String> executeQuery(Connection connection, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization
		ArrayList<String> result = new ArrayList<String>();
		
		// Query with AutoCloseable interface introduced in Java 7. 
		// Hence statements and result sets are closed with the end of try block.
		// See: https://blogs.oracle.com/WebLogicServer/entry/using_try_with_resources_with
	    try (Statement stmt = connection.createStatement(); 
	    	 ResultSet rs = stmt.executeQuery(sql)) {
	    	
			while (rs.next()) {
				result.add(rs.getString(1)); // Columns in ResultSets are indexed from 1
			}
			
			// Log it
			// Remove line breaks and collapse all "whitespace substrings" longer than one character.
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " "); 		
			logger.debug(sql);
		} catch (SQLException e) {
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
			logger.info(e.getMessage() + " | " + sql);
			try {
				boolean isConnected = connection.isValid(1000);
				if (!isConnected) logger.warn("The connection doesn't appear to be open.");
			} catch (SQLException ignored) {}
		} 
	    
	    // Technical: Do not close the connection here. The connection is closed at the end of the Launcher.
	    
	    return result;
    }

    
    // Return true, if the result set is empty.
	public static boolean isResultSetEmpty(Connection connection, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization to a default value
		boolean result = true;
	
		// Query using try-with-resources
	    try (Statement stmt = connection.createStatement();
	    	 ResultSet rs = stmt.executeQuery(sql)) {
			
	    	// Even if the query would return several rows, only the first row is transmitted 
			stmt.setMaxRows(1); 
			
			// If there is the first row, the result set is not empty.
			if(rs.next()) {
				  result = false;
			}
			
			// Log it
			// Remove line breaks and collapse all "whitespace substrings" longer than one character.
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " "); 		
			logger.debug(sql);
		} catch (SQLException e) {
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
			logger.info(e.getMessage() + " | " + sql);
			try {
				boolean isConnected = connection.isValid(1000);
				if (!isConnected) logger.warn("The connection doesn't appear to be open.");
				// Attempt to reconnect thirty times. If all the attempts are unsuccessful, terminate the application.
				// Otherwise, roll back. And try the command again.
				// Note: watch out for server timeouts - we do not to end up in an infinite loop.
				// Note: give it some time (1s) to recover from temporary database outages, such as those which occur during a database restart or brief loss of network connectivity. 
				// connection.rollback();
				// connection.setAutoCommit(false);
			} catch (SQLException ignored) {}
		} 
	    
	    // Technical: Do not close the connection here. The connection is closed at the end of the Launcher.
	    
	    return result;
    }        
	
	
}
