package connection;

import org.junit.Assert;
import org.junit.Test;
import run.Setting;

import java.util.ArrayList;
import java.util.List;


public class ParserTest {

	private Setting setting = new Setting();

	@Test
	public void testObs_end() {
		// Setting for SAS
		setting.limitSyntax = "obs";
		String sql = "select col1 from tab1";
		int rowCount = 10;

		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1(obs=10)";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testObs_middle() {
		// Setting
		setting.limitSyntax = "obs";
		String sql = "select col1 from tab1 where col2 = 'orchid'";
		int rowCount = 10;

		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1(obs=10) where col2 = 'orchid'";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRowNum_zeroOccurrence() {
		// Setting
		setting.limitSyntax = "rownum";
		String sql = "select col1 from t1";
		int rowCount = 10;

		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from t1 WHERE ROWNUM <= 10";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRowNum_oneOccurrence() {
		// Setting
		setting.limitSyntax = "rownum";
		String sql = "select col1 from tab1 where col1 <> 'a'";
		int rowCount = 10;

		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from tab1 WHERE ROWNUM <= 10 AND col1 <> 'a'";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRowNum_twoOccurrences() {
		// Setting
		setting.limitSyntax = "rownum";
		String sql = "select col1 from (select col1 from t1 where col2 = 'b') t2 where col1 <> 'a'";
		int rowCount = 10;

		// Test
		String actual = Parser.limitResultSet(setting, sql, rowCount);
		String expected = "select col1 from (select col1 from t1 where col2 = 'b') t2 WHERE ROWNUM <= 10 AND col1 <> 'a'";
		Assert.assertEquals(expected, actual);
	}


	@Test
	public void expandBase_table_date() {
		// Setting
		setting.targetDate = "date";
		String sql = "SELECT t1.@base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT t1.@baseId, t1.@baseDate, t1.@baseTarget, t1.col1 FROM t1";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase_date() {
		// Setting
		setting.targetDate = "date";
		String sql = "SELECT @base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT @baseId, @baseDate, @baseTarget, t1.col1 FROM t1";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase_table() {
		// Setting
		setting.targetDate = null;
		String sql = "SELECT t1.@base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT t1.@baseId, t1.@baseTarget, t1.col1 FROM t1";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase() {
		// Setting
		setting.targetDate = null;
		String sql = "SELECT @base, t1.col1 FROM t1";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT @baseId, @baseTarget, t1.col1 FROM t1";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void expandBase_multiple() {
		// Setting
		setting.targetDate = null;
		String sql = "SELECT @base FROM t1 GROUP BY @base";

		// Test
		String actual = Parser.expandBase(setting, sql);
		String expected = "SELECT @baseId, @baseTarget FROM t1 GROUP BY @baseId, @baseTarget";
		Assert.assertEquals(expected, actual);
	}


	@Test
	public void main() {
		String sql = "select apple, t1.@baseId, t2.orange, t2.@baseId, t2.@baseTarget, @baseId from t1";
		List<String> idList = new ArrayList<>();
		idList.add("id1");
		idList.add("id2");
		setting.quoteEntityClose = "`";
		setting.quoteEntityOpen = "`";

		String actual = Parser.expandToList(setting, sql, "@baseId", idList);
		String expected = "select apple, t1.`id1`, t1.`id2`, t2.orange, t2.`id1`, t2.`id2`, t2.@baseTarget, `id1`, `id2` from t1";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void replaceExistsSAS() {
		// Setting
		setting.supportsSelectExists = false;
		setting.databaseVendor= "SAS";
		String sql = "SELECT EXISTS(...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT COUNT(*)>0 FROM (...)";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void replaceExistsMSSQL() {
		// Setting
		setting.supportsSelectExists = false;
		setting.databaseVendor= "Microsoft SQL Server";
		String sql = "SELECT EXISTS(...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT CASE WHEN (EXISTS (...)) THEN 1 ELSE 0 END";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void replaceExistsNot() {
		// Setting
		setting.supportsSelectExists = true;
		String sql = "SELECT EXISTS(...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT EXISTS(...)";
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void replaceExistsSpace() {
		// Setting
		setting.supportsSelectExists = false;
		String sql = "Select  Exists  (...)";

		// Test
		String actual = Parser.replaceExists(setting, sql);
		String expected = "SELECT COUNT(*)>0 FROM   (...)";
		Assert.assertEquals(expected, actual);
	}


	@Test
	public void corr() {
		setting.corrSyntax = "corr(@column1, @column2)";
		String code = "SELECT corr(colA,colB) FROM tab1";
		String expected = "SELECT corr(colA, colB) FROM tab1";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void corrOfCorr() {
		setting.corrSyntax = "corr(@column1, @column2)";
		String code = "SELECT corr(colA,corr(colB,colC)) FROM tab1";
		String expected = "SELECT corr(colA, corr(colB,colC)) FROM tab1";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void corrMySQL() {
		setting = new Setting("MySQL", "mutagenesis");
		String code = "SELECT corr(col1,col2) FROM tab1";
		String expected = "SELECT coalesce((Avg(col1 * col2) - Avg(col1) * Avg(col2)) / nullif((stdDev_pop(col1) * stdDev_pop(col2)), 0), 0) FROM tab1";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void corrMSSQL() {
		setting = new Setting("Azure", "mutagenesis");
		String code = "SELECT corr(col1,col2) FROM tab1";
		String expected = "SELECT coalesce((Avg(col1 * col2) - Avg(col1) * Avg(col2)) / nullif((stdevp(col1) * stdevp(col2)), 0), 0) FROM tab1";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}


	@Test
	public void timeDiffMultiple() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT col1 FROM tab1 WHERE dateDiff(col2,col3) > 10 AND dateDiff(col4,col5) < 5";
		String expected = "SELECT col1 FROM tab1 WHERE (col2 - col3) > 10 AND (col4 - col5) < 5";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void timeDiffLineWrap() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT col1 FROM tab1 WHERE dateDiff(col2,\ncol3) > 10";
		String expected = "SELECT col1 FROM tab1 WHERE (col2 - \ncol3) > 10";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void timeDiffFunction() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT @baseId, DATEDIFF(@baseDate,max(@temporalColumn, @baseId))";
		String expected = "SELECT @baseId, (@baseDate - max(@temporalColumn, @baseId))";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void timeDiffInFunction() {
		setting.dateDiffSyntax = "(@dateTo - @dateFrom)";
		String code = "SELECT @baseId, @aggregateFunction(DATEDIFF(@baseDate,@temporalColumn))";
		String expected = "SELECT @baseId, @aggregateFunction((@baseDate - @temporalColumn))";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void timeDiffMySQL() {
		setting.dateDiffSyntax = "DATEDIFF(@dateTo, @dateFrom)";
		String code = "DATEDIFF(@baseDate,@temporalColumn)";
		String expected = "DATEDIFF(@baseDate, @temporalColumn)";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void timeDateToNumberOracle() {
		setting.dateToNumber = "(@column - TO_DATE('01011970','DDMMYYYY'))";
		String code = "SELECT DateToNumber(@column) FROM t1";
		String expected = "SELECT (@column - TO_DATE('01011970','DDMMYYYY')) FROM t1";
		String actual = Parser.getDialectCode(setting, code);

		Assert.assertEquals(expected, actual);
	}
}