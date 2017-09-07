package connection;

import meta.OutputTable;
import org.apache.log4j.Logger;
import parser.ANTLR;
import run.Setting;

import java.util.HashMap;
import java.util.Map;

// Oracle does not support "exists" clause in the select part.
// We deal with that by using counts.
public final class SQLOracle extends SQL {
	// Logging
	private static final Logger logger = Logger.getLogger(SQLOracle.class.getName());

	// Returns true if the column contains null.
	public boolean containsNull(Setting setting, String table, String column) {
		String sql = "SELECT count(*) FROM @inputTable WHERE @column is null";

		sql = Parser.replaceExists(setting, sql);
		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@inputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) > 0;
	}

	// Returns true if the date column contains a date from the future.
	// Each derived column in Teradata must have an implicit name.
	public boolean containsFutureDate(Setting setting, String table, String column) {
		String sql = "SELECT count(*) FROM (SELECT 1 AS col1 FROM @inputTable WHERE {fn NOW()} < @column) t";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		Map<String, String> fieldMap = new HashMap<>();
		fieldMap.put("@column", column);
		fieldMap.put("@inputTable", table);
		sql = escapeEntityMap(setting, sql, fieldMap);

		sql = ANTLR.parseSQL(setting, sql);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) > 0;
	}

	// Get the maximal cardinality of the table in respect to targetId. If the cardinality is 1:1,
	// we may want to remove the bottom time constrain in base propagation.
	// Note that we are working with the input tables -> alter commands are forbidden.
	// Each derived column in Teradata must have an implicit name.
	public boolean isIdUnique(Setting setting, OutputTable table) {
		String sql = "SELECT count(*) FROM (" +
				"SELECT count(*) AS cnt " +
				"FROM @inputTable " +
				"GROUP BY @idCommaSeparated " +
				"HAVING count(*)>1" +
				") t";

		// Get escape characters
		String QL = setting.quoteEntityOpen;
		String QR = setting.quoteEntityClose;

		// Escape ids
		String idCommaSeparated = "";
		for (String id : table.propagationForeignConstraint.fColumn) {
			idCommaSeparated += QL + id + QR + ",";
		}
		idCommaSeparated = idCommaSeparated.substring(0, idCommaSeparated.length() - 1);
		sql = sql.replace("@idCommaSeparated", idCommaSeparated);

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, "dummy");
		sql = escapeEntityTable(setting, sql, table);

		boolean result = Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) == 0;

		if (result)
			logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " doesn't contain duplicates #");
		else
			logger.trace("# Column " + table.propagationForeignConstraint.fColumn + " in " + table.originalName + " CONTAINS duplicates #");

		return result;
	}

	// Check whether the columns {baseId, baseDate} are unique in the table in the inputSchemaList.
	public boolean isTargetTupleUnique(Setting setting, String table) {

		String sql = "SELECT count(*)" +
				"FROM (" +
				"SELECT @targetId " +
				"FROM @targetTable " +
				"GROUP BY @targetId" + (setting.targetDate == null ? " " : ", @targetDate ") +
				"HAVING count(*)>1" +
				") t";

		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) == 0;
	}

	// Returns 1 if the baseId in the table in the outputSchema is unique.
	// It appears this can be disk space demanding (tested in Accidents dataset)
	public boolean isTargetIdUnique(Setting setting, String table) {
		String sql = "SELECT count(*) FROM (" +
				"SELECT @baseId FROM @outputTable GROUP BY @baseId HAVING count(*)>1) t";


		sql = expandName(sql);
		sql = escapeEntity(setting, sql, table);

		return Integer.valueOf(Network.executeQuery(setting.dataSource, sql).get(0)) == 0;
	}

}

