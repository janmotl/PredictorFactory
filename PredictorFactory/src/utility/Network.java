package utility;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;

import run.Setting;



public final class Network {
	

	
	// Return the connection and database configuration
	public static Setting getConnection(Setting setting) {
        
        // Load the configuration file from XML
		NetworkPropertyList driverList = NetworkPropertyList.unmarshall();  
		Assert.assertEquals(driverList.getJDBCProperties("MySQL").driver_class, "com.mysql.jdbc.Driver");  
        
        // Get the configuration for the specified database  
        NetworkProperty props = driverList.getJDBCProperties(setting.dbType);
        
		// Build a DataSource
        BasicDataSource ds = new BasicDataSource();   
		ds.setDriverClassName(props.driver_class);
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
		setting.quoteMarks = props.quote_marks;
		setting.isCreateTableAsCompatible = "yes".equals(props.create_table_as_compatible);
		setting.isSchemaCompatible = "yes".equals(props.schema_compatible);
		
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
			//e.printStackTrace();
			//System.out.println("SQL state: " + e.getSQLState()); // ANSI code
 			//System.out.println(e.getMessage()); // String
 			logToConsole("ERROR", e.getMessage(), sql);
 			//log(connection, SQL.addToStatementJournal(setting, sql, e.getSQLState(), e.getMessage()));
		} 
	    
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
			//e.printStackTrace();
			//System.out.println("SQL state: " + e.getSQLState()); // ANSI code
 			//System.out.println(e.getMessage()); // String
			logToConsole("ERROR", e.getMessage(), sql);
		}
	    
	    return result;
    }

    
    // UNFORTUNATELLY, JAVA DOESN'T SUPPORT DYNAMIC TYPING DURING THE RUNTIME. 
    // ADD NEW FUNCTIONS FOR INT (row count, cardinality) & SINGLE DOUBLE (Chi2)
    
    
    // Subroutine: Log the event to the console
    private static void logToConsole(String outcome, String errorMessage, String sql)  {
    	sql = sql.replaceAll("\\s+", " "); 		// Collapse all "whitespace substrings" longer than one character
    	
    	String message = StringUtils.rightPad(outcome, 6) +  "| " + 
    					 errorMessage + " | " +
    					 sql;
    	
    	System.out.println(message);
    }
    
    
    

}
