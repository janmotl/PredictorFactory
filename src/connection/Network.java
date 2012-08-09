package connection;


import com.google.common.base.MoreObjects;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import run.Setting;
import utility.JDBCCompliance;

import javax.sql.DataSource;
import java.io.Console;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public final class Network {
	// Logging
	private static final Logger logger = Logger.getLogger(Network.class.getName());
	
	// Return the connection and database configuration
	// Note: We are not using driver's DataSource, because their interface for setting URL is variable.
	// Note: DBCP2 doesn't work anymore with SAS driver, which is only JDBC 3.0 compliant.
	// See: www.mchange.com/projects/c3p0/
	// NOTE: This function should permit passing of URL to deal with nonstandard situations (like domain login to MSSQL)
	// Throws runtime exception if the connection can't be established (can be caught thought)
	public static Setting openConnection(Setting setting) {
		
		// Build a DataSource
		setting.dataSource = new HikariDataSource();
		setting.dataSource.setDriverClassName(setting.driverClass);	// Old-school necessity for pre-JDBC 4.0 drivers
		setting.dataSource.setUsername(setting.username);
		setting.dataSource.setPassword(setting.password);
		if (setting.url == null) {
			setting.dataSource.setJdbcUrl(setting.urlPrefix + setting.host + ":" + setting.port
					+ setting.dbNameSeparator + setting.database + setting.urlSuffix); // Database is required by Azure
		} else {
			setting.dataSource.setJdbcUrl(setting.url);
		}
		//setting.dataSource.setLeakDetectionThreshold(10000);	// Awesome for debugging, but may produce false positives
		setting.dataSource.setMaximumPoolSize(2);
		//ds.setConnectionTimeout(1000); // Fail fast: 3 seconds (for detail error messages disable it)

		// Define a validation query for drivers that do not provide isValid() API.
		// Do not set it for JDBC 4.0 compliant drivers as it would slow them down.
		// See: https://github.com/brettwooldridge/HikariCP
		if (setting.testQuery != null) {
			setting.dataSource.setConnectionTestQuery(setting.testQuery);
		}
		
		// If the connectionProperty doesn't contain username and/or password, prompt the user.
		// Note: The only way how to mask the password (that I am aware of) is to ask for the password 
		// during the runtime. And when we are asking for the password, we may also ask for the username, if necessary.
		try (Scanner input = new Scanner(System.in)) {	// The issue with scanner is that once it's closed it can't be reopened.
			if (setting.username == null) {
				System.out.print("Enter your username for the database: ");
				setting.dataSource.setUsername(input.nextLine());
			}
			if (setting.password == null) {
				System.out.print("Enter your password for the database: ");
				Console console = System.console();
				if (console == null) {	// In Eclipse IDE "console" doesn't work. Use regular System.in instead.
					setting.dataSource.setPassword(input.nextLine());
				} else { 				// Outside Eclipse IDE passwords are masked as expected. 
					setting.dataSource.setPassword(new String(console.readPassword()));
				}
			}
		}
		 
		// Connect to the server
		logger.debug("Connecting to the server with the following URL: " + setting.dataSource.getJdbcUrl());
		String quoteEntity = "  ";
		
		try (Connection connection = setting.dataSource.getConnection()) {
			// Log metadata 
			java.sql.DatabaseMetaData metaData = connection.getMetaData();
			logger.debug("Database product name: " + metaData.getDatabaseProductName());
			logger.debug("Database version: " + metaData.getDatabaseProductVersion());
			logger.debug("Driver name: " + metaData.getDriverName());
			logger.debug("Driver version: " + metaData.getDriverVersion());
			logger.debug("Driver JDBC compliance: " + JDBCCompliance.getDriverVersion(metaData));
			logger.debug("Identifier quote string: " + metaData.getIdentifierQuoteString());
			logger.debug("Maximum count of characters in a column name: " + metaData.getMaxColumnNameLength());
			logger.debug("Maximum count of characters in a table name: " + metaData.getMaxTableNameLength());
			logger.debug("Maximum count of columns in a table: " + metaData.getMaxColumnsInTable());
			logger.debug("Maximum row size in bytes: " + metaData.getMaxRowSize());

			// Store collected metadata
			setting.identifierLengthMax = Math.min(metaData.getMaxColumnNameLength(), metaData.getMaxTableNameLength());
			setting.predictorMaxTheory = estimateFeatureLimitCount(setting, metaData.getMaxColumnsInTable(), metaData.getMaxRowSize());
			setting.predictorMax = MoreObjects.firstNonNull(setting.predictorMax, Math.min(setting.predictorMaxTheory, setting.sampleCount));
			quoteEntity = metaData.getIdentifierQuoteString();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			// We want to be able to read the error message in GUI, but we do not want to be forced to catch it.
			throw new RuntimeException(e);
		}

		logger.info("#### Successfully connected to the database ####");

		// Set default values if they are not provided 
		setting.quoteEntityOpen = MoreObjects.firstNonNull(setting.quoteEntityOpen, quoteEntity.substring(0, 1));
		setting.quoteEntityClose = MoreObjects.firstNonNull(setting.quoteEntityClose, quoteEntity.substring(0, 1));
	
		return setting;
	}
     
	// If we are opening the connection here, we have to close the connection here.
	public static void closeConnection(Setting setting) {
		setting.dataSource.close();
	}
	
	// Execute update. Returns true if the update was successful. False otherwise.
    public static boolean executeUpdate(DataSource dataSource, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization
		boolean isOk = false;
	
		// Query using try-with-resources
	    try (Connection connection = dataSource.getConnection();
			 Statement stmt = connection.createStatement()) {
	    	
	    	stmt.executeUpdate(sql);
			isOk = true; 
			
			// Remove line breaks and collapse all "whitespace substrings" longer than one character.
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " "); 		
			logger.debug(sql);
		} catch (SQLException e) {
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
			logger.info(e.getMessage() + " | " + sql);
		}
	    
	    return isOk;
    }
    
 	// Get list of strings
    public static List<String> executeQuery(DataSource dataSource, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization
		ArrayList<String> result = new ArrayList<>();
		
		// Query with AutoCloseable interface introduced in Java 7. 
		// Hence statements and result sets are closed with the end of try block.
		// See: https://blogs.oracle.com/WebLogicServer/entry/using_try_with_resources_with
	    try (Connection connection = dataSource.getConnection();
			 Statement stmt = connection.createStatement();
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
		} 

	    return result;
    }

	// Get a single bool. This a useful convenience function because some databases
	// return t/f, other true/false and another 1/0. And SAS returns 1.0/0.0.
	// The only reliable way how to deal with this variety to use rs.getBoolean().
	public static Boolean isTrue(DataSource dataSource, String sql){
		// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}

		// Initialization
		Boolean result = null;

		// Query with AutoCloseable interface introduced in Java 7.
		// Hence statements and result sets are closed with the end of try block.
		// See: https://blogs.oracle.com/WebLogicServer/entry/using_try_with_resources_with
		try (Connection connection = dataSource.getConnection();
			 Statement stmt = connection.createStatement();
			 ResultSet rs = stmt.executeQuery(sql)) {

			// Even if the query would return several rows, only the first row is transmitted
			stmt.setMaxRows(1);

			// If there is the first row, the result set is not empty.
			if(rs.next()) {
				result = rs.getBoolean(1);
			}

			// Log it
			// Remove line breaks and collapse all "whitespace substrings" longer than one character.
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
			logger.debug(sql);
		} catch (SQLException e) {
			sql = sql.replace("\n", " ").replace("\r", " ").replaceAll("\\s+", " ");
			logger.info(e.getMessage() + " | " + sql);
		}

		return result;
	}

    // Return true, if the result set is empty.
	public static boolean isResultSetEmpty(DataSource dataSource, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization to a default value
		boolean result = true;
	
		// Query using try-with-resources
	    try (Connection connection = dataSource.getConnection();
			 Statement stmt = connection.createStatement();
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
		} 

	    return result;
    }

	// Estimate the maximal safe amount of features that we can produce.
	// This estimate is upper bounded by maxColumnsInTable (e.g. 1600 for PostgreSQL)
	// and maxRowSize (e.g. 64kB for MySQL). If a passed parameter is null or 0, the relevant
	// constraint is not applied.
	private static int estimateFeatureLimitCount(Setting setting, Integer maxColumnsInTable, Integer maxRowSize) {
		// Default limit
		int result = Integer.MAX_VALUE;

		// Reserve columns for {targetId, targetDate, targetColumn}
		int reserved = setting.targetIdList.size() + (setting.targetDate==null?0:1) + 1;

		if (maxColumnsInTable != null && maxColumnsInTable > 0) {
			result = Math.min(result, maxColumnsInTable - reserved);
		}

		// BigInt is composed of 8 bytes -> divide by 8. But numerical can be bigger. The average size of a column
		// on NCAA dataset is 8.6 byte -> divide by 10. This is just a likely upper bound - if varchars were
		// returned, a single in table varchar could take the whole space.
		// Also, reserve ~100 bytes for database table overhead.
		if (maxRowSize != null && maxRowSize > 0) {
			result = Math.min(result, (maxRowSize-100)/10 - reserved);
		}

		// While PostgreSQL reports a maximal row size to be 1073741824 bytes, in reality, we likely sooner hit
		// Postgre's limit on block size, which is by default set to 8kB (8192 bytes) per row.
		// Unfortunately, this value is not reported by JDBC.
		// To stop worrying about the limit on Postgre, let's just set a safe value of 250 columns
		// (https://www.postgresql.org/about/) - the maximal count of columns that we are (more or less) guaranteed
		// to be creatable.
		if ("PostgreSQL".equals(setting.databaseVendor)) {
			result = 250 - reserved;
		}

		logger.debug("The maximal amount of returned predictors is: " + result);

		return result;
	}
	
}
