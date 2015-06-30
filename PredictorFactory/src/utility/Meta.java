package utility;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import run.Setting;

public class Meta {
	
	// Logging
	public static final Logger logger = Logger.getLogger(Meta.class.getName());

	// Define struct. 
	// Always create collections to avoid null pointer exception and need to create collections at many places.
	// SortedSet is used to make selection of a single element easy to write.
	public static class Table {
		public SortedSet<String> idColumn = new TreeSet<String>();					// Foreign and primary keys
		public SortedSet<String> nominalColumn = new TreeSet<String>();				// Categorical columns
		public SortedSet<String> numericalColumn = new TreeSet<String>();			// Additive columns 
		public SortedSet<String> timeColumn = new TreeSet<String>();				// Time, date, datetime, timestamp...
		public List<List<String>> relationship = new ArrayList<List<String>>();		// List of {thatTable, thisColumn, thatColumn} 
		public boolean isUnique;													// Does combination {baseId, baseDate} repeat?
		public Map<String, List<String>> uniqueList = new TreeMap<String, List<String>>(); // Map of {columnName, unique value list}
		
		@Override 
		public String toString() {
		    StringBuilder result = new StringBuilder();
		    String NEW_LINE = System.getProperty("line.separator");

		    result.append("Table {" + NEW_LINE);
		    result.append(" idColumn: " + idColumn + NEW_LINE);
		    result.append(" nominalColumn: " + nominalColumn + NEW_LINE);
		    result.append(" numericalColumn: " + numericalColumn + NEW_LINE );
		    result.append(" timeColumn: " + timeColumn + NEW_LINE);
		    result.append("}");

		    return result.toString();
		 }
	}

	
	
	// 0) Get list of all schemas.
	// POSSIBLY I COULD ASSUME THAT: database = setting.database
	// HENCE ELIMINATE ONE OF THE PARAMETERS
	public static SortedSet<String> collectSchemas(Setting setting, String database) {

		// Initialization
		SortedSet<String> schemaSet = new TreeSet<String>();
		
		// If supports only catalogs (MySQL) -> get all catalogs
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			try (ResultSet rs = setting.connection.getMetaData().getCatalogs()) {
				while (rs.next()) {
					String schemaName = rs.getString("TABLE_CAT");
					schemaSet.add(schemaName);
				}
			} catch (SQLException ignored) {}
		} 
		
		// If supports only schemas (SAS) -> get all schemas
		if (!setting.supportsCatalogs && setting.supportsSchemas) {
			try (ResultSet rs = setting.connection.getMetaData().getSchemas()) {
				while (rs.next()) {
					String schemaName = rs.getString("TABLE_SCHEM");
					
					if ("SAS".equals(setting.databaseVendor)) {
						schemaName = schemaName.replace(" ", "");	// Remove space padding
					}
					
					schemaSet.add(schemaName);
				}
			} catch (SQLException ignored) {}
		} 
		
		// If supports catalogs and schemas -> get all schemas in the specified catalog
		if (setting.supportsCatalogs && setting.supportsSchemas) {
			try (ResultSet rs = setting.connection.getMetaData().getSchemas(database, "%")) {
				while (rs.next()) {
					String schemaName = rs.getString("TABLE_SCHEM");
					schemaSet.add(schemaName);
				}
			} catch (SQLException ignored) {}
		}

		// QC schema count
		if (schemaSet.isEmpty()) {
			logger.warn("The count of available schemas is 0.");
		}
		
		return schemaSet;
	}
	
	// 1) Get all tables in the schema.
	public static SortedSet<String> collectTables(Setting setting, String database, String schema) {
		// Deal with different combinations of catalog/schema support
		// MySQL type
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			database = schema;
			schema = null;
		}
		
		// SAS type
		if (!setting.supportsCatalogs && setting.supportsSchemas) {
			database = null;
		}
				
		// Initialization
		SortedSet<String> tableSet = new TreeSet<String>();
		String[] tableType = {"TABLE"};
		
		// Get all the columns in the table using try-with-resources.
		try (ResultSet rs = setting.connection.getMetaData().getTables(database, schema, "%", tableType)) {

			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				
				if ("SAS".equals(setting.databaseVendor)) {
					tableName = tableName.replace(" ", "");	// Remove space padding
				}
				
				tableSet.add(tableName);
			}
		} catch (SQLException ignored) {}
		
		// QC table count
		if (tableSet.isEmpty()) {
			logger.warn("The count of available tables in " + database + "." + schema + " is 0.");
		}
		
		return tableSet;
	}
	
	// 2) Get all columns in the table. Return <ColumnName, DataType>.
	public static SortedMap<String, Integer> collectColumns(Setting setting, String database, String schema, String table) {
		// If the database doesn't support schemas, use schema name as database name (java treats
		// schema-less databases differently than SQL databases do)
		if (!setting.supportsCatalogs) {
			database = schema;
			schema = null;
		}
		
		// Initialization
		SortedMap<String, Integer> columnMap = new TreeMap<String, Integer>();
		
		// Get all the columns in the table using try-with-resources.
		try (ResultSet rs = setting.connection.getMetaData().getColumns(database, schema, table, null)) {

			while (rs.next()) {
				// Note: "Type Name" column contains vendor specific name -> the
				// strings are changing.
				// Hence "Data Type" column is used. It has the advantage that
				// there is a finite set of these numbers.
				int dataType = rs.getInt("DATA_TYPE");
				String columnName = rs.getString("COLUMN_NAME");
				
				// Oracle decided that NVARCHAR2 should be classified as "other" type (1111)
				// even though it can be casted to String. Hence do the work that Oracle
				// should have done.
				if (dataType == 1111 && rs.getString("TYPE_NAME").toUpperCase().contains("CHAR")) {
					dataType = 12; // Treat it as VARCHAR2
				}
				
				// PostgreSQL classifies interval as "other" type (1111). Change the classification to time data type.
				if (dataType == 1111 && rs.getString("TYPE_NAME").toUpperCase().contains("INTERVAL")) {
					dataType = 93; // Treat it as timestamp
				}
				
				columnMap.put(columnName, dataType);
			}
		} catch (SQLException ignored) {}
		
		return columnMap;
	}
	
	// 3) Get numerical, nominal and time columns.
	public static Table categorizeColumns(Table table, Map<String, Integer> column, String tableName) {
		// Parameter control
		if (column == null || column.isEmpty()) {
			logger.info("Table: " + tableName + " doesn't contain any column beside ids.");
			return table;
		}
		
		// Initialization
		SortedSet<String> string = new TreeSet<String>();
		SortedSet<String> numerical = new TreeSet<String>();
		SortedSet<String> time = new TreeSet<String>();
		int columnCounter = 0;
		
		// Categorize the dataTypes. If the data type is not recognized, the column is ignored.
		// This is intentional because we do not know how to deal with blobs (dataType = 2004)...
		// And we don't want them to slow down the mining process -> do not propagate them.
		// Doc: https://docs.oracle.com/cd/A97337_01/ias102_otn/buslog.102/bc4j/bc_abcdatatypes.htm
		// Doc: http://alvinalexander.com/java/edu/pj/jdbc/recipes/ResultSet-ColumnType.shtml
		// Doc: http://docs.oracle.com/javase/8/docs/api/constant-values.html#java.sql.Types.ARRAY
		for (String columnName : column.keySet()) {
			int dataType = column.get(columnName); // THIS COULD POTENTIONALLY RESULT IN NULL POINTER EXCEPTION
			
			if (dataType == -16 || dataType == -15 || dataType == -9 || dataType == -1 || dataType == 1 || dataType == 12) {
				string.add(columnName);
			} else if ((dataType >= -7 && dataType <= -5) || (dataType >= 2 && dataType <= 8) || dataType == 16) {
				numerical.add(columnName);
			} else if (dataType == 91 || dataType == 92 || dataType == 93 || dataType == 2013 || dataType == 2014) {
				time.add(columnName);
			} else {
				columnCounter++;
				logger.debug("Ignoring column: " + tableName + "." + columnName 
						+ " because it is of an unsupported data type (" + dataType + ")");
			}
		}
		
		// Output quality control
		if (columnCounter > 0) {
			logger.warn("In total " + columnCounter + " columns were ignored in " + tableName + " because of unsopperted data type.");
		}

		// Setters
		// THE ASSIGNMENT IS NOT EXACT AS SOME NUMERICAL COLUMNS CAN BE NOMINAL...
		table.numericalColumn = numerical;
		table.nominalColumn = string;
		table.timeColumn = time;
		
		return table;
	}
}
