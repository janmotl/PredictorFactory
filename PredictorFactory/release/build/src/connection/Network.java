package connection;


import java.io.Console;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import run.Setting;

import com.zaxxer.hikari.HikariDataSource;



public final class Network {
	// Logging
	private static final Logger logger = Logger.getLogger(Network.class.getName());
	
	// Return the connection and database configuration
	// Note: We are not using driver's DataSource, because their interface for setting URL is variable.
	// Note: DBCP2 doesn't work anymore with SAS driver, which is only JDBC 3.0 compliant.
	// Note: c3p0 is a viable option. But may require setting fine tuning.
	// NOTE: Have to change the logic to benefit from pooling (Recovery from database outages)
	// See: http://http://www.mchange.com/projects/c3p0/
	// NOTE: This function doesn't need databaseProperty anymore -> move it away
	// NOTE: This function should permit passing of URL to deal with nonstandard situations (like domain login to MSSQL) 
	public static Setting openConnection(Setting setting, String connectionPropertyName, String databasePropertyName) {
		
        // Load the connection configuration from a XML
		ConnectionPropertyList connectionPropertyList = ConnectionPropertyList.unmarshall();
		DriverPropertyList driverPropertyList = DriverPropertyList.unmarshall(); 
		DatabasePropertyList databasePropertyList = DatabasePropertyList.unmarshall();
        
        // Get the configuration for the specified database vendor		
        ConnectionProperty connectionProperty = connectionPropertyList.getConnectionProperties(connectionPropertyName);
        DriverProperty driverProperty = driverPropertyList.getDriverProperties(connectionProperty.driver);
        DatabaseProperty databaseProperty = databasePropertyList.getDatabaseProperties(databasePropertyName);
        
		// Build a DataSource
        @SuppressWarnings("resource")	// WILL HAVE TO CHANGE
		HikariDataSource ds = new HikariDataSource();
		ds.setDriverClassName(driverProperty.driverClass);	// Old-school necessity for pre-JDBC 4.0 drivers 
		ds.setUsername(connectionProperty.username);
		ds.setPassword(connectionProperty.password);
		String url = driverProperty.urlPrefix + connectionProperty.host + ":" + connectionProperty.port
					+ driverProperty.dbNameSeparator + connectionProperty.database; // Database is required by Azure
		ds.setJdbcUrl(url);
		
		// Define a validation query for drivers that do not provide isValid() API.
		// Do not set it for JDBC 4.0 compliant drivers as it would slow them down.
		// See: https://github.com/brettwooldridge/HikariCP
		if (driverProperty.testQuery != null) {
			ds.setConnectionTestQuery(driverProperty.testQuery);
		}
		
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
		logger.debug("Connecting to the server with the following URL: " + url);
		String quoteEntity = "  ";
		
		try {
			setting.connection = ds.getConnection();
			
			// Log metadata 
			java.sql.DatabaseMetaData metaData = setting.connection.getMetaData();
			logger.debug("Database product name: " + metaData.getDatabaseProductName());
			logger.debug("Driver version: " + metaData.getDriverVersion());
			logger.debug("Maximum number of characters for a column name: " + metaData.getMaxColumnNameLength());
			logger.debug("Maximum number of characters in a table name: " + metaData.getMaxTableNameLength());
			logger.debug("Maximum number of columns in a table: " + metaData.getMaxColumnsInTable());
			logger.debug("Identifier quote string: " + metaData.getIdentifierQuoteString());
			
			// Store collected metadata
			setting.identifierLengthMax = Math.min(metaData.getMaxColumnNameLength(), metaData.getMaxTableNameLength());
			setting.columnMax = metaData.getMaxColumnsInTable();
			quoteEntity = metaData.getIdentifierQuoteString();
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			System.exit(1); // Likely wrong credentials -> gracefully close the application
		}
		
		logger.info("#### Successfully connected to the database ####");
		
		// Set other settings
		if (setting.columnMax==0) {
			setting.columnMax = Integer.MAX_VALUE;	// Databases with unlimited column count returns 0 -> use big default.
		}
		
		if (driverProperty.quoteEntityOpen != null) {
			setting.quoteEntityOpen = driverProperty.quoteEntityOpen;
			setting.quoteEntityClose = driverProperty.quoteEntityClose;
		} else {
			setting.quoteEntityOpen = quoteEntity.substring(0, 1);		// Use what's provided by the driver
			setting.quoteEntityClose = quoteEntity.substring(0, 1);	
		}
		
		if (driverProperty.quoteAliasOpen != null) {
			setting.quoteAliasOpen = driverProperty.quoteAliasOpen;
			setting.quoteAliasClose = driverProperty.quoteAliasClose;
		} else {
			setting.quoteAliasOpen = "\"";	// The default 
			setting.quoteAliasClose = "\"";
		}
			
		if (driverProperty.indexNameSyntax != null) {
			setting.indexNameSyntax = driverProperty.indexNameSyntax;
		} else {
			setting.indexNameSyntax = "table_idx";
		}
			
			
		setting.database = connectionProperty.database;
		setting.databaseVendor = connectionProperty.driver;
		setting.supportsCatalogs = driverProperty.supportsCatalogs;
		setting.supportsSchemas = driverProperty.supportsSchemas;
		setting.supportsCreateTableAs = driverProperty.supportsCreateTableAs;
		setting.supportsWithData = driverProperty.supportsWithData;
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
		setting.limitSyntax = driverProperty.limitSyntax;
		setting.randomCommand = driverProperty.randomCommand;
		
		setting.inputSchema = databaseProperty.inputSchema;
		setting.outputSchema = databaseProperty.outputSchema;
		setting.targetId = databaseProperty.targetId;
		setting.targetDate = databaseProperty.targetDate;
		setting.targetColumn = databaseProperty.targetColumn;
		setting.targetTable = databaseProperty.targetTable;
		setting.blackListTable = databaseProperty.blackListTable;
		setting.blackListColumn = databaseProperty.blackListColumn;
		
		setting.unit = databaseProperty.unit;
		setting.lag = databaseProperty.lag;
		setting.lead = databaseProperty.lead;
		setting.task = databaseProperty.task;
		setting.blackListPattern = databaseProperty.blackListPattern;
		
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
