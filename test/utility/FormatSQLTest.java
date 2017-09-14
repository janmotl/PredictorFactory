package utility;

import org.junit.Assert;
import org.junit.Test;

public class FormatSQLTest {

	// By itself this test works, but if executed after MainAppTest, it ends up with:
	//	    Java has been detached already, but someone is still trying to use it at -[GlassApplication applicationWillResignActive:]
	// It seems to be a recurring problem on OsX:
	//  https://bugs.openjdk.java.net/browse/JDK-8113976
	//  https://bugs.openjdk.java.net/browse/JDK-8114065
	//  https://bugs.openjdk.java.net/browse/JDK-8116226
	// It looks like MainApp termination is not kosher.
	@Test
	public void formatSQL() throws Exception {
		String sql = "SELECT base1, Mode() WITHIN GROUP (ORDER BY column3) FROM propagatedTable1 GROUP BY base1";
		String actual = FormatSQL.formatSQL(sql);

		//FormatSQLinForm.displayHtmlInBrowser(actual);

		Assert.assertTrue(actual.startsWith("<"));
	}

}