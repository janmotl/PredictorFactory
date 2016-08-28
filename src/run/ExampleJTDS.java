package run;


import java.sql.*;

public class ExampleJTDS {

	public static void main(String[] args) {

		// Create a variable for the connection string.
		String connectionUrl = "jdbc:jtds:sqlserver://hlt3qa7607.database.windows.net:1433/PredictorFactory;ssl=require";
		String user = "yzan@hlt3qa7607";
		String pass = "Ty nejsi admin!";

		// Declare the JDBC objects.
		Connection conn = null;

		try {
			// Establish the connection.
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			conn = DriverManager.getConnection(connectionUrl, user, pass);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}