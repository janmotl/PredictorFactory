package run;


import java.sql.Connection;
import java.sql.DriverManager;

public class ExamplePostgreSQL {

	public static void main(String[] args) {

		// Create a variable for the connection string.
		String connectionUrl = "jdbc:pgsql://localhost:5432/PredictorFactory";
		String user = "jan";
		String pass = "";

		// Declare the JDBC objects.
		Connection conn = null;

		try {
			// Establish the connection.
			Class.forName("com.impossibl.postgres.jdbc.PGDriver");
			conn = DriverManager.getConnection(connectionUrl, user, pass);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}