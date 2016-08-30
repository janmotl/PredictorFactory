package connection;

import org.junit.Assert;
import org.junit.Test;

import run.Setting;



public class ParserTest {


	@Test
	public void testObs_end() {
		// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "obs";
		String sql = "select col1 from tab1";
		int rowCount = 10;


		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1(obs=10)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testObs_middle() {
		// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "obs";
		String sql = "select col1 from tab1 where col2 = 'orchid'";
		int rowCount = 10;


		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1(obs=10) where col2 = 'orchid'";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRowNum_zeroOccurrence() {
		// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "rownum";
		String sql = "select col1 from t1";
		int rowCount = 10;


		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from t1 WHERE ROWNUM <= 10";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRowNum_oneOccurrence() {
		// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "rownum";
		String sql = "select col1 from tab1 where col1 <> 'a'";
		int rowCount = 10;


		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1 WHERE ROWNUM <= 10 AND col1 <> 'a'";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRowNum_twoOccurrences() {
		// Setting
		Setting setting = new Setting();
		setting.limitSyntax = "rownum";
		String sql = "select col1 from (select col1 from t1 where col2 = 'b') t2 where col1 <> 'a'";
		int rowCount = 10;


		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from (select col1 from t1 where col2 = 'b') t2 WHERE ROWNUM <= 10 AND col1 <> 'a'";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}


	@Test
	public void expandBase_table_date() {
		// Setting
		Setting setting = new Setting();
		setting.targetDate = "date";
		String sql = "SELECT t1.@base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT t1.@baseId, t1.@baseDate, t1.@baseTarget, t1.col1 FROM t1";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase_date() {
		// Setting
		Setting setting = new Setting();
		setting.targetDate = "date";
		String sql = "SELECT @base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT @baseId, @baseDate, @baseTarget, t1.col1 FROM t1";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase_table() {
		// Setting
		Setting setting = new Setting();
		setting.targetDate = null;
		String sql = "SELECT t1.@base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT t1.@baseId, t1.@baseTarget, t1.col1 FROM t1";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase() {
		// Setting
		Setting setting = new Setting();
		setting.targetDate = null;
		String sql = "SELECT @base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT @baseId, @baseTarget, t1.col1 FROM t1";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase_multiple() {
		// Setting
		Setting setting = new Setting();
		setting.targetDate = null;
		String sql = "SELECT @base FROM t1 GROUP BY @base";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT @baseId, @baseTarget FROM t1 GROUP BY @baseId, @baseTarget";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}


	@Test
	public void replaceExistsSAS() {
		// Setting
		Setting setting = new Setting();
		setting.supportsSelectExists = false;
		setting.databaseVendor= "SAS";
		String sql = "SELECT EXISTS(...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT COUNT(*)>0 FROM (...)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void replaceExistsMSSQL() {
		// Setting
		Setting setting = new Setting();
		setting.supportsSelectExists = false;
		setting.databaseVendor= "Microsoft SQL Server";
		String sql = "SELECT EXISTS(...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT CASE WHEN (EXISTS (...)) THEN 1 ELSE 0 END";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void replaceExistsNot() {
		// Setting
		Setting setting = new Setting();
		setting.supportsSelectExists = true;
		String sql = "SELECT EXISTS(...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT EXISTS(...)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void replaceExistsSpace() {
		// Setting
		Setting setting = new Setting();
		setting.supportsSelectExists = false;
		String sql = "Select  Exists  (...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT COUNT(*)>0 FROM   (...)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}
}