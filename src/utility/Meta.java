package utility;

import meta.Column;
import meta.ForeignConstraint;
import meta.ForeignConstraintList;
import meta.Table;
import org.apache.log4j.Logger;
import run.Setting;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Meta {

	// Logging
	private static final Logger logger = Logger.getLogger(Meta.class.getName());

	// 1) Get list of all schemas.
	// POSSIBLY I COULD ASSUME THAT: database = setting.database
	// HENCE ELIMINATE ONE OF THE PARAMETERS (and we are already passing setting...)
	public static SortedSet<String> collectSchemas(Setting setting, String database) {

		// Initialization
		SortedSet<String> schemaSet = new TreeSet<>(new NaturalOrderComparator());

		// If supports only catalogs (MySQL) -> get all catalogs
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			try (Connection connection = setting.dataSource.getConnection();
			     ResultSet rs = connection.getMetaData().getCatalogs()) {

				while (rs.next()) {
					String schemaName = rs.getString("TABLE_CAT");
					schemaSet.add(schemaName);
				}
			} catch (SQLException ignored) {
			}
		}

		// If supports only schemas (SAS) -> get all schemas
		if (!setting.supportsCatalogs && setting.supportsSchemas) {
			try (Connection connection = setting.dataSource.getConnection();
			     ResultSet rs = connection.getMetaData().getSchemas()) {

				while (rs.next()) {
					String schemaName = rs.getString("TABLE_SCHEM");

					if ("SAS".equals(setting.databaseVendor)) {
						schemaName = schemaName.trim(); // Remove space padding
					}

					schemaSet.add(schemaName);
				}
			} catch (SQLException ignored) {
			}
		}

		// If supports catalogs and schemas -> get all schemas in the specified catalog
		if (setting.supportsCatalogs && setting.supportsSchemas) {
			try (Connection connection = setting.dataSource.getConnection();
			     ResultSet rs = connection.getMetaData().getSchemas(database, "%")) {

				while (rs.next()) {
					String schemaName = rs.getString("TABLE_SCHEM");
					schemaSet.add(schemaName);
				}
			} catch (SQLException ignored) {
			}
		}

		// QC schema count
		if (schemaSet.isEmpty()) {
			logger.warn("The count of available schemas is 0.");
		}

		return schemaSet;
	}

	// 2) Get all tables and views in the schema.
	public static SortedMap<String, Table> collectTables(Setting setting, String database, String schema) {
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
		SortedMap<String, Table> tableMap = new TreeMap<>(new NaturalOrderComparator());
		String[] tableType = {"TABLE", "VIEW", "MATERIALIZED VIEW"};    // Ignore system tables...

		// Get all the tables using try-with-resources.
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet rs = connection.getMetaData().getTables(database, schema, "%", tableType)) {

			while (rs.next()) {
				Table table = new Table();
				table.name = rs.getString("TABLE_NAME");

				if ("SAS".equals(setting.databaseVendor)) {
					table.name = table.name.trim(); // Remove tail space padding
				}

				tableMap.put(table.name, table);
			}
		} catch (SQLException ignored) {
		}

		// QC table count
		if (tableMap.isEmpty()) {
			logger.warn("The count of available tables in " + database + "." + schema + " is 0.");
		}

		return tableMap;
	}

	// 3) Get all columns in the table.
	public static SortedMap<String, Column> collectColumns(Setting setting, String database, String schema, String table) {
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
		SortedMap<String, Column> columnMap = new TreeMap<>(new NaturalOrderComparator());

		// Get all the columns in the table using try-with-resources.
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet rs = connection.getMetaData().getColumns(database, schema, table, null)) {

			while (rs.next()) {
				Column column = new Column(rs.getString("COLUMN_NAME"));
				column.dataType = rs.getInt("DATA_TYPE");
				column.dataTypeName = rs.getString("TYPE_NAME");
				column.isNullable = "YES".equals(rs.getString("IS_NULLABLE"));      // WHAT IF INDIFFERENT?

				// SAS stores entity names in chars instead of in varchars
				if ("SAS".equals(setting.databaseVendor)) {
					column.name = column.name.trim();   // Remove space padding
				}

				// Oracle decided that NVARCHAR2 should be classified as "other" type (1111)
				// even though it can be casted to String. Hence do the work that Oracle
				// should have done.
				if (column.dataType == 1111 && rs.getString("TYPE_NAME").toUpperCase().contains("CHAR")) {
					column.dataType = 12; // Treat it as VARCHAR2
				}

				// PostgreSQL classifies interval as "other" type (1111). Change the classification to time data type.
				if (column.dataType == 1111 && rs.getString("TYPE_NAME").toUpperCase().contains("INTERVAL")) {
					column.dataType = 93; // Treat it as timestamp
				}

				columnMap.put(column.name, column);
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		}

		return columnMap;
	}

	// 4) Get all relationships related to the table.
	public static List<ForeignConstraint> collectRelationships(Setting setting, String schema, String table) {
		List<ForeignConstraint> relationshipList = downloadRelationships(setting, schema, table);
		List<ForeignConstraint> result = new ArrayList<>();

		// Sort by {fTable, name, sequence}
		Collections.sort(relationshipList);

		// Assume that the elements in relationshipList are ordered by {fTable, sequence}
		for (ForeignConstraint fc : relationshipList) {
			if (result.contains(fc)) {
				ForeignConstraint constrainReference = result.get(result.size() - 1);
				constrainReference.column.addAll(fc.column);
				constrainReference.fColumn.addAll(fc.fColumn);
			} else {
				result.add(fc);
			}
		}

		// Add relevant relationships from an XML file, if available.
		// It is ugly that we read and parse the XML repeatedly. But should not be a bottleneck.
		List<ForeignConstraint> relationshipXML = ForeignConstraintList.unmarshall("foreignConstraint.xml").getForeignConstraintList(table);
		if (!relationshipXML.isEmpty()) {
			// NOTE: Could replace list with a LinkedHasSet to avoid duplicates (use: Refactor | Type Migration)
			result.addAll(relationshipXML);
			logger.info("Table " + table + " has " + relationshipXML.size() + " relationships defined in the XML file.");
		}

		return result;
	}

	// 5) Get the single primary key (It would be the best if only artificial keys were returned. At least
	// we are excluding the composite keys).
	public static String getPrimaryKey(Setting setting, String database, String schema, String table) {
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
		List<String> primaryKeyList = new ArrayList<>();

		// Get all columns making the primary key
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet rs = connection.getMetaData().getPrimaryKeys(database, schema, table)) {

			while (rs.next()) {
				String primaryKey = rs.getString("COLUMN_NAME");

				// SAS stores entity names in chars instead of in varchars
				if ("SAS".equals(setting.databaseVendor)) {
					primaryKey = primaryKey.replace(" ", "");    // Remove space padding (should remove just leading/trailing spaces if enough)
				}

				primaryKeyList.add(primaryKey);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		// If the table contains a PK composed of exactly one column, return the name of the column
		if (primaryKeyList.size() == 1) {
			return primaryKeyList.get(0);
		}

		// Otherwise return null;
		return null;
	}


	// Subroutine: Get all relationships for the table.
	// Composite relationships are represented by multiple records with the same "name".
	private static List<ForeignConstraint> downloadRelationships(Setting setting, String schema, String table) {
		String database;

		// Deal with catalog/schema less databases
		// MySQL
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			database = schema;
			schema = null;
		} else {
			database = setting.database;
		}

		// SAS driver doesn't return keys. Use own query.
		if ("SAS".equals(setting.databaseVendor)) {
			return downloadRelationshipsSAS(setting, schema, table);
		}


		// Initialization
		List<ForeignConstraint> relationshipList = new ArrayList<>();

		// Get imported keys
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet resultSet = connection.getMetaData().getImportedKeys(database, schema, table)) {
			while (resultSet.next()) {
				ForeignConstraint relationship = new ForeignConstraint();
				relationship.name = resultSet.getString("FK_NAME");
				relationship.table = table;
				relationship.fTable = resultSet.getString("PKTABLE_NAME");
				relationship.column.add(resultSet.getString("FKCOLUMN_NAME"));
				relationship.fColumn.add(resultSet.getString("PKCOLUMN_NAME"));
				relationship.sequence = resultSet.getShort("KEY_SEQ");
				relationshipList.add(relationship);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		// And exported keys
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet resultSet = connection.getMetaData().getExportedKeys(database, schema, table)) {
			while (resultSet.next()) {
				ForeignConstraint relationship = new ForeignConstraint();
				relationship.name = resultSet.getString("FK_NAME");
				relationship.table = table;
				relationship.fTable = resultSet.getString("FKTABLE_NAME");
				relationship.column.add(resultSet.getString("PKCOLUMN_NAME"));
				relationship.fColumn.add(resultSet.getString("FKCOLUMN_NAME"));
				relationship.sequence = resultSet.getShort("KEY_SEQ");
				relationshipList.add(relationship);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		// Output Quality Control
		if (relationshipList.isEmpty()) {
			logger.info("Table " + table + " doesn't have any predefined relationship in the database.");
		}

		return relationshipList;
	}

	// Subroutine: SAS JDBC driver doesn't return keys. Use dictionary tables instead.
	// See: www2.sas.com/proceedings/sugi30/070-30.pdf
	// HAVE TO CHANGE THE QUERY TO INCLUDE FK_NAME AND SEQUENCE.
	private static List<ForeignConstraint> downloadRelationshipsSAS(Setting setting, String schema, String table) {
		// Initialization
		List<ForeignConstraint> relationshipList = new ArrayList<>();
		String sql = "select t1.memname as FKTABLE_NAME " +
				", t1.unique_memname as PKTABLE_NAME " +
				", t2.column_name as FKCOLUMN_NAME " +
				", t3.column_name as PKCOLUMN_NAME " +
				"from dictionary.REFERENTIAL_CONSTRAINTS t1 " +
				"join dictionary.CONSTRAINT_COLUMN_USAGE t2 " +
				"on t1.libname = t2.table_catalog " +
				"and t1.memname = t2.table_name " +
				"and t1.constraint_name = t2.constraint_name " +
				"join dictionary.CONSTRAINT_COLUMN_USAGE t3 " +
				"on t1.unique_libname = t3.table_catalog " +
				"and t1.unique_memname = t3.table_name " +
				"and t1.unique_constraint_name = t3.constraint_name ";


		// Get all relations coming from this table
		try (Connection connection = setting.dataSource.getConnection();
		     Statement stmt = connection.createStatement()) {

			String condition = "where t1.libname = '" + schema + "' and t1.memname = '" + table + "'";
			ResultSet rs = stmt.executeQuery(sql + condition);

			while (rs.next()) {
				ForeignConstraint relationship = new ForeignConstraint();
				relationship.name = rs.getString("FK_NAME");
				relationship.table = table;
				relationship.fTable = rs.getString("PKTABLE_NAME");
				relationship.column.add(rs.getString("FKCOLUMN_NAME"));
				relationship.fColumn.add(rs.getString("PKCOLUMN_NAME"));
				relationshipList.add(relationship);
			}

			// And now Exported keys
			condition = "where t1.unique_libname = '" + schema + "' and t1.unique_memname = '" + table + "'";
			rs = stmt.executeQuery(sql + condition);

			while (rs.next()) {
				ForeignConstraint relationship = new ForeignConstraint();
				relationship.name = rs.getString("FK_NAME");
				relationship.table = table;
				relationship.fTable = rs.getString("FKTABLE_NAME");
				relationship.column.add(rs.getString("PKCOLUMN_NAME"));
				relationship.fColumn.add(rs.getString("FKCOLUMN_NAME"));
				relationshipList.add(relationship);
			}

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		return relationshipList;
	}

}
