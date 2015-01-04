package utility;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;

import run.Setting;



public final class Network {
	

	
	// Return the connection and database configuration
	// WOULDN'T be java DataSource BE ENOUGH?
	public static Setting getConnection(Setting setting) {
        
        // Load the configuration file from XML
		NetworkPropertyList driverList = NetworkPropertyList.unmarshall();   
        
        // Get the configuration for the specified database  
        NetworkProperty props = driverList.getJDBCProperties(setting.dbType);
        
		// Build a DataSource
        BasicDataSource ds = new BasicDataSource();   
		ds.setDriverClassName(props.driverClass);
		ds.setUrl(props.url);
		ds.setUsername(props.username);
		ds.setPassword(props.password);

		// Connect to the server
		try {
			setting.connection = ds.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// Set other settings
		setting.quoteMarks = props.quoteMarks;
		setting.isCreateTableAsCompatible = "yes".equals(props.createTableAsCompatible);
		setting.isSchemaCompatible = "yes".equals(props.schemaCompatible);
		setting.dateAddSyntax = props.dateAddSyntax;
		setting.stdDevCommand = props.stdDevCommand;
		setting.dateTimeCompatible = "yes".equals(props.dateTimeCompatible);
		
		return setting;
	}
     
	
	// Execute update. Returns true if the update was successful. False otherwise.
    public static boolean executeUpdate(Connection connection, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization
		boolean isOk = false;
	
		// Query
	    try {
			Statement stmt = connection.createStatement();
			stmt.executeUpdate(sql);
			isOk = true; 
			logToConsole("OK", "", sql);
		} catch (SQLException e) {
 			logToConsole("ERROR", e.getMessage(), sql);
 			//log(connection, SQL.addToStatementJournal(setting, sql, e.getSQLState(), e.getMessage()));
		}
	    
	    // Technical: Do not close the connection here. The connection is closed at the end of the Launcher. 
	    
	    return isOk;
    }
    
    
 	// Get list of strings
    public static ArrayList<String> executeQuery(Connection connection, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization
		ArrayList<String> result = new ArrayList<String>();
	
		// Query
	    try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			
			while (rs.next()) {
				result.add(rs.getString(1)); // DataSource is indexed from 1
			}
			logToConsole("OK", "", sql);
		} catch (SQLException e) {
			logToConsole("ERROR", e.getMessage(), sql);
		} 
	    
	    // Technical: Do not close the connection here. The connection is closed at the end of the Launcher.
	    
	    return result;
    }

    
    // Get scalar boolean. 
    // If the query fails, null is returned.
    // It is useful to have a special function for this as some databases return f/t while other return 0/1.
    public static Boolean getBoolean(Connection connection, String sql){
    	// Parameter checking
		if (StringUtils.isBlank(sql)) {
			throw new IllegalArgumentException("SQL statement is required");
		}
		
		// Initialization to a default value
		Boolean result = null;
	
		// Query
	    try {
			Statement stmt = connection.createStatement();
			stmt.setMaxRows(1); // Even if the query would return several rows, only the first row is transmitted 
			ResultSet rs = stmt.executeQuery(sql);
			
			if(rs.next()) {
				  result = rs.getBoolean(1); // DataSource is indexed from 1
			}
			
			logToConsole("OK", "", sql);
		} catch (SQLException e) {
			logToConsole("ERROR", e.getMessage(), sql);
		} 
	    
	    // Technical: Do not close the connection here. The connection is closed at the end of the Launcher.
	    
	    return result;
    }
   
    // UNFORTUNATELLY, JAVA DOESN'T SUPPORT DYNAMIC TYPING DURING THE RUNTIME. 
    // ADD NEW FUNCTIONS FOR A SINGLE INT (row count) & SINGLE DOUBLE (Chi2) & SINGLE BOOLEAN (isUnique)
    
    
    // Subroutine: Log the event to the console
    private static void logToConsole(String outcome, String errorMessage, String sql)  {
    	sql = sql.replaceAll("\\s+", " "); 		// Collapse all "whitespace substrings" longer than one character
    	
    	String message = StringUtils.rightPad(outcome, 6) +  "| " + 
    					 errorMessage + " | " +
    					 sql;
    	
    	System.out.println(message);
    }
    
    
    

}
