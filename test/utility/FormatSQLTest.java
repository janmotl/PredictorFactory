package utility;

import org.junit.Assert;
import org.junit.Test;

public class FormatSQLTest {

	@Test
	public void formatSQL() throws Exception {
		String sql = "SELECT base1, Mode() WITHIN GROUP (ORDER BY column3) FROM propagatedTable1 GROUP BY base1";
		String actual = FormatSQL.formatSQL(sql);

		//FormatSQLinForm.displayHtmlInBrowser(actual);

		Assert.assertTrue(actual.startsWith("<"));
	}

}