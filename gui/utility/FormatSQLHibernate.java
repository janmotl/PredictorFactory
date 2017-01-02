package utility;

import org.hibernate.engine.jdbc.internal.BasicFormatterImpl;

public class FormatSQLHibernate {

	public static void main(String[] args) {
		String sql = "SELECT base1, mode() WITHIN GROUP (ORDER BY column3) " +
				"   FROM propagatedTable1 " +
				"   GROUP BY base1";

		BasicFormatterImpl sqlFormatter = new BasicFormatterImpl();
		String formattedSql = sqlFormatter.format(sql);
		System.out.print(formattedSql);
	}
}
