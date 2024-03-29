package utility;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import meta.*;
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
			} catch (SQLException e) {
				logger.warn(e.getMessage());
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
			} catch (SQLException e) {
				logger.warn(e.getMessage());
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
			} catch (SQLException e) {
				logger.warn(e.getMessage());
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
		// Deal with different combinations of catalog/schema support in JDBC drivers
		String driversDatabase = database;
		String driversSchema = schema;

		// MySQL type
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			driversDatabase = schema;
			driversSchema = null;
		}

		// SAS type
		if (!setting.supportsCatalogs && setting.supportsSchemas) {
			driversDatabase = null;
		}

		// Initialization
		SortedMap<String, Table> tableMap = new TreeMap<>(new NaturalOrderComparator());
		String[] tableType = {"TABLE", "VIEW", "MATERIALIZED VIEW"};    // Ignore system tables...

		// Get all the tables using try-with-resources.
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet rs = connection.getMetaData().getTables(driversDatabase, driversSchema, "%", tableType)) {

			while (rs.next()) {
				String name = rs.getString("TABLE_NAME");

				if ("SAS".equals(setting.databaseVendor)) {
					name = name.trim(); // Remove tail space padding
				}

				Table table = new Table(schema, name);
				tableMap.put(table.name, table);
			}
		} catch (SQLException e) {
			logger.warn(e.getMessage());
		}

		return tableMap;
	}

	// 3) Get all columns in the table.
	public static SortedMap<String, Column> collectColumns(Setting setting, String database, String schema, String table) {
		// Deal with different combinations of catalog/schema support in JDBC drivers
		String driversDatabase = database;
		String driversSchema = schema;

		// MySQL type
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			driversDatabase = schema;
			driversSchema = null;
		}

		// SAS type
		if (!setting.supportsCatalogs && setting.supportsSchemas) {
			driversDatabase = null;
		}

		// Initialization
		SortedMap<String, Column> columnMap = new TreeMap<>(new NaturalOrderComparator());

		// Get all the columns in the table using try-with-resources.
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet rs = connection.getMetaData().getColumns(driversDatabase, driversSchema, table, null)) {

			while (rs.next()) {
				String name = rs.getString("COLUMN_NAME");
				int dataType = rs.getInt("DATA_TYPE");
				String dataTypeName = rs.getString("TYPE_NAME");
				boolean isNullable = "YES".equals(rs.getString("IS_NULLABLE"));      // WHAT IF INDIFFERENT?
				boolean isDecimal = rs.getInt("DECIMAL_DIGITS")>0;

				// SAS stores entity names in chars instead of in varchars
				if ("SAS".equals(setting.databaseVendor)) {
					name = name.trim();   // Remove space padding
				}

				// Oracle decided that NVARCHAR2 and VARCHAR2 should be classified as OTHER type (1111)
				// even though it can be casted to String. Hence do the work that Oracle should have done.
				if (dataType == 1111 && rs.getString("TYPE_NAME").toUpperCase().equals("NVARCHAR2")) {
					dataType = -9; // Treat it as NVARCHAR
				}
				if (dataType == 1111 && rs.getString("TYPE_NAME").toUpperCase().equals("VARCHAR2")) {
					dataType = 12; // Treat it as VARCHAR
				}

				// MSSQL decided that XML should be classified as LONGNVARCHAR (-16).
				// But it does not seem to matter.
				if (dataType == -16 && rs.getString("TYPE_NAME").toUpperCase().equals("XML")) {
					dataType = 2009; // Treat it as SQLXML
				}

				// Create the column with non-null schema name
				Column column = new Column(schema, table, name, dataType, dataTypeName, isNullable, isDecimal);
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

		// Reconstruct compound relationships by assuming that the elements in relationshipList
        // are ordered by {fTable, sequence}.
		for (ForeignConstraint fc : relationshipList) {
			if (result.contains(fc)) {
				ForeignConstraint constrainReference = result.get(result.size() - 1);
				constrainReference.column.addAll(fc.column);
				constrainReference.fColumn.addAll(fc.fColumn);
			} else {
				result.add(fc);
			}
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

	// 6) Get unique constrained columns
	public static List<String> getUniqueColumns(Setting setting, String database, String schema, String table) {
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
		Map<String, String> uniques = new HashMap<>();
		Multiset<String> multiset = HashMultiset.create();

		// Get unique indexes
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet rs = connection.getMetaData().getIndexInfo(database, schema, table, true, true)) {  // Only unique indexes, approximations are ok
			while (rs.next()) {
				String columnName = rs.getString("COLUMN_NAME");
				String constraintName = rs.getString("INDEX_NAME");
				if (columnName == null)
					continue; // Azure is quirky and returns one row with only nulls -> ignore the quirky row
				uniques.put(constraintName, columnName);
				multiset.add(constraintName);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		}

		// We want unique constraints per individual columns, not per set of columns
		List<String> result = new ArrayList<>();
		for (String constraintName : multiset) {
			if (multiset.count(constraintName) == 1) {
				result.add(uniques.get(constraintName));
			}
		}

		return result;
	}


	// Subroutine: Get all relationships for the table.
	// Composite relationships are represented by multiple records with the same "name".
	private static List<ForeignConstraint> downloadRelationships(Setting setting, String schema, String table) {
		// Deal with different combinations of catalog/schema support in JDBC drivers
		String driversDatabase = setting.database;
		String driversSchema = schema;

		// MySQL type
		if (setting.supportsCatalogs && !setting.supportsSchemas) {
			driversDatabase = schema;
			driversSchema = null;
		}

		// SAS driver doesn't return keys. Use own query.
		if ("SAS".equals(setting.databaseVendor)) {
			return downloadRelationshipsSAS(setting, schema, table);
		}


		// Initialization
		List<ForeignConstraint> relationshipList = new ArrayList<>();

		// Get imported keys
		try (Connection connection = setting.dataSource.getConnection();
		     ResultSet resultSet = connection.getMetaData().getImportedKeys(driversDatabase, driversSchema, table)) {
			while (resultSet.next()) {
				ForeignConstraint relationship = new ForeignConstraint();
				relationship.name = resultSet.getString("FK_NAME");
				if (setting.supportsSchemas) {
					relationship.schema = resultSet.getString("FKTABLE_SCHEM");
					relationship.fSchema = resultSet.getString("PKTABLE_SCHEM");
				} else {
					relationship.schema = resultSet.getString("FKTABLE_CAT");   // MySQL does not support schemas but catalogs...
					relationship.fSchema = resultSet.getString("PKTABLE_CAT");
				}
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
		     ResultSet resultSet = connection.getMetaData().getExportedKeys(driversDatabase, driversSchema, table)) {
			while (resultSet.next()) {
				ForeignConstraint relationship = new ForeignConstraint();
				relationship.name = resultSet.getString("FK_NAME");
				if (setting.supportsSchemas) {
					relationship.schema = resultSet.getString("FKTABLE_SCHEM");
					relationship.fSchema = resultSet.getString("PKTABLE_SCHEM");
				} else {
					relationship.schema = resultSet.getString("FKTABLE_CAT");   // MySQL does not support schemas but catalogs...
					relationship.fSchema = resultSet.getString("PKTABLE_CAT");
				}
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
				relationship.fTable = rs.getString("PKTABLE_NAME"); // Note ITENTIONAL swapping of fTable and PKTABLE
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

	private static ForeignConstraint reverseFCDirection(ForeignConstraint fc) {
		return new ForeignConstraint(fc.name, fc.schema, fc.table, fc.fSchema, fc.fTable, fc.column, fc.fColumn);
	}

	// Add reverse directions
	public static List<ForeignConstraint> addReverseDirections(Collection<ForeignConstraint> relationships) {
		List<ForeignConstraint> undirected = new ArrayList<>(relationships);

		for (ForeignConstraint foreignConstraint : relationships) {
			undirected.add(reverseFCDirection(foreignConstraint));
		}

		return undirected;
	}

    // Returns all FC related to the table.
	// But if there is a self-referencing FC, include that FC just once.
	// NOTE: Shouldn't it also check the schemas?
	public static List<ForeignConstraint> getTableForeignConstraints(List<ForeignConstraint> foreignConstraints, String tableName) {
		List<ForeignConstraint> result = new ArrayList<>();

		// Select the appropriate foreign constraints
		for (ForeignConstraint foreignConstraint : foreignConstraints) {
			if (foreignConstraint.table.equals(tableName)) {
				result.add(foreignConstraint);
			} else if (foreignConstraint.fTable.equals(tableName)) {
				result.add(foreignConstraint);
			}
		}

		return result;
	}
}
