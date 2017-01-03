package utility;


import org.jetbrains.annotations.NotNull;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCCompliance {

	// Try to estimate the driver's compliance
	public static String getDriverVersion(DatabaseMetaData metaData) {

		// Test presence of a JDBC 2.1 feature
		try {
			metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY);
		} catch (Throwable ignored) {
			return "1.0";
		}

		// Test presence of a JDBC 3.0 feature
		try {
			metaData.supportsSavepoints();
		} catch (Throwable ignored) {
			return "2.1";
		}

		// JDBC 3.0 introduced a call for getting the JDBC version
		try {
			return metaData.getJDBCMajorVersion() + "." + metaData.getJDBCMinorVersion();
		} catch (SQLException ignored) {
		}

		return "?";
	}

}
