package antlr;

import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import parser.ANTLR;
import run.Setting;


public class ANTLRTest {

	@NotNull private Setting setting = new Setting("MySQL", "financial");

	@Before
	public void initialization() {
		setting.supportsJoinUsing = false;
	}

	@Test
	public void corr() {
		String sql = "select corr(col1,col2) from tab1";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select ((Sum(col1 * col2) - count(*) * Avg(col1) * Avg(col2)) / ((count(*) - 1) * (stdDev_samp(col1) * stdDev_samp(col2)))) from tab1";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void dateDiff() {
		String sql = "select dateDiff(col1,col2) from tab1";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select DATEDIFF(col1, col2) from tab1";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void dateToNumber() {
		String sql = "select dateToNumber(col1) from tab1";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select UNIX_TIMESTAMP(col1) from tab1";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void using() {
		String sql = "select * from t1 join t2 using(id1,id2)";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select * from t1 join t2 on (t1.id1=t2.id1 AND t1.id2=t2.id2)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void usingAlias() {
		String sql = "select * from table1 \"t1\" join table2 t2 using(\"id1\",id2)";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select * from table1 \"t1\" join table2 t2 on (\"t1\".\"id1\"=t2.\"id1\" AND \"t1\".id2=t2.id2)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void usingSubquery() {
		String sql = "select * from (select * from t1 join t2 using(id1)) t3 join table2 t4 using(id2,id3)";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select * from (select * from t1 join t2 on (t1.id1=t2.id1)) t3 join table2 t4 on (t3.id2=t4.id2 AND t3.id3=t4.id3)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void usingSpaces() {
		String sql = "SELECT column1 FROM table1 t1 JOIN table2 USING ( id1 , id2 ) GROUP BY column2";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "SELECT column1 FROM table1 t1 JOIN table2 on (t1.id1=table2.id1  AND  t1.id2=table2.id2) GROUP BY column2";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void usingBasePartitionBy() {
		String sql = "SELECT column1 FROM table1 t1 JOIN table2 t2 USING (@basePartitionBy)";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "SELECT column1 FROM table1 t1 JOIN table2 t2 on (t1.@baseId = t2.@baseId AND t1.@baseDate = t2.@baseDate)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void usingBase() {
		String sql = "SELECT column1 FROM table1 t1 JOIN table2 t2 USING (@base)";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "SELECT column1 FROM table1 t1 JOIN table2 t2 on (t1.@baseId = t2.@baseId AND t1.@baseDate = t2.@baseDate AND t1.@baseTarget = t2.@baseTarget)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void crossJoin() {
		String sql = "select * from table1 t1 cross join table2 t2"; // We test that On/Using clause in join is not required
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select * from table1 t1 cross join table2 t2";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void fullOuterJoin() {
		String sql = "select * from table1 full outer join table2"; // We test that multiple texts can be in front of join
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select * from table1 full outer join table2";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void allTogether() {
		String sql = "select dateToNumber(col1), dateDiff(col1,col2), corr(col1,col2) from tab1 join tab2 using(id)";
		String actual = ANTLR.parseSQL(setting, sql);
		String expected = "select UNIX_TIMESTAMP(col1), DATEDIFF(col1, col2), ((Sum(col1 * col2) - count(*) * Avg(col1) * Avg(col2)) / ((count(*) - 1) * (stdDev_samp(col1) * stdDev_samp(col2)))) from tab1 join tab2 on (tab1.id=tab2.id)";
		System.out.println(actual);
		Assert.assertEquals(expected, actual);
	}

}