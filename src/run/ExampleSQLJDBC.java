package run;


import java.io.IOException;
import java.sql.*;
import java.util.logging.*;

public class ExampleSQLJDBC {




//  public static void main(String[] args) {
//
//      // Setting.
//      String connectionUrl = "jdbc:sqlserver://hlt3qa7607.database.windows.net:1433;database=PredictorFactory;encrypt=true;trustServerCertificate=true;";
//      //connectionUrl = "jdbc:sqlserver://hlt3qa7607.database.windows.net:1433;database=PredictorFactory;user=yzan@hlt3qa7607;password={Ty nejsi admin!};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
//      String user = "yzan@hlt3qa7607";
//      String pass = "Ty nejsi admin!";
//
//      // Declare the JDBC object.
//      Connection conn = null;
//
//      try {
//          // Establish the connection.
//          Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//          conn = DriverManager.getConnection(connectionUrl, user, pass);
//      }
//      catch (Exception e) {
//          e.printStackTrace();
//      }
//  }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
        logger.setLevel(Level.FINE);
        logger.addHandler(new StreamHandler(System.out, new SimpleFormatter()));
        FileHandler fh;

        try {

            // This block configure the logger with handler and formatter
            fh = new FileHandler("./MyLogFile.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            // the following statement is used to log any messages
            logger.info("My first log");

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String hostName = "hlt3qa7607";
        String dbName = "PredictorFactory";
        String user = "yzan@hlt3qa7607";
        String password = "Ty nejsi admin!";
        String url = String.format("jdbc:sqlserver://%s.database.windows.net:1433;database=%s;user=%s;password=%s;encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;", hostName, dbName, user, password);
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select 1+1 as sum");
        while(rs.next()) {
            System.out.println(rs.getInt("sum"));
        }
        rs.close();
        stat.close();
        conn.close();
    }
}

