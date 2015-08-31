package connection;

import metaInformation.ForeignConstraint;
import metaInformation.MetaOutput;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.util.ArrayList;

public class SQLTest {

    private MetaOutput.OutputTable table;

    @Before
    public void initialization() {
        table = new MetaOutput.OutputTable();
        table.originalName = "trans";
        table.propagatedName = "propagated_trans";
        table.propagationTable = "propagated_account";

        ForeignConstraint fc = new ForeignConstraint();
        table.foreignConstraint = fc;
    }


    @Test
    public void propagateID_simple() {
        // Setting
        Setting setting = new Setting("MSSQL Jan", "financial");
        setting.quoteEntityOpen = "[";
        setting.quoteEntityClose = "]";

        // Parameters
        table.foreignConstraint.column = new ArrayList<>();
        table.foreignConstraint.fColumn = new ArrayList<>();
        table.foreignConstraint.column.add("account_id");
        table.foreignConstraint.fColumn.add("account_id");

        // Test
        String actual =  SQL.propagateID(setting, table);
        String expected = "SELECT t1.[propagated_id1], t1.[propagated_date], t1.[propagated_target], t1.[propagated_fold], t2.* INTO [predictor_factory].[propagated_trans] FROM [predictor_factory].[propagated_account] t1 INNER JOIN [financial].[trans] t2 ON t1.[account_id] = t2.[account_id]";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void propagateID_composite() {
        // Setting
        Setting setting = new Setting("MSSQL Jan", "financial");
        setting.quoteEntityOpen = "[";
        setting.quoteEntityClose = "]";
        setting.baseIdList.add("propagated_id2");

        // Parameters
        table.foreignConstraint.column = new ArrayList<>();
        table.foreignConstraint.fColumn = new ArrayList<>();
        table.foreignConstraint.column.add("account_id1");
        table.foreignConstraint.fColumn.add("account_id2");
        table.foreignConstraint.column.add("account_id3");
        table.foreignConstraint.fColumn.add("account_id4");

        // Test
        String actual =  SQL.propagateID(setting, table);
        String expected = "SELECT t1.[propagated_id1], t1.[propagated_id2], t1.[propagated_date], t1.[propagated_target], t1.[propagated_fold], t2.* INTO [predictor_factory].[propagated_trans] FROM [predictor_factory].[propagated_account] t1 INNER JOIN [financial].[trans] t2 ON t1.[account_id1] = t2.[account_id2] AND t1.[account_id3] = t2.[account_id4]";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void addCreateTableAs1() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test user variable compliance (tolerance to at-sign). Important as I am using at-signs in patterns.
        String result = SQL.addCreateTableAs(setting, "SELECT @column FROM table");
        Assert.assertEquals("SELECT @column INTO @outputTable FROM table", result);
    }

    @Test
    public void addCreateTableAs2() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test ignorance of a table called "from". This is important in the maintable assembly.
        String result = SQL.addCreateTableAs(setting, "SELECT [from] FROM table");
        Assert.assertEquals("SELECT [from] INTO @outputTable FROM table", result);
    }

    @Test
    public void addCreateTableAs3() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test for case insensitivity. As the SQL Keywords are case-insensitive in SQL 92.
        String result = SQL.addCreateTableAs(setting, "SELECT col FrOm table");
        String expected = "SELECT col INTO @outputTable FrOm table";
        Assert.assertEquals(expected.toLowerCase(), result.toLowerCase());
    }

    @Test
    public void addCreateTableAs4() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test with subquery in the select part.
        String result = SQL.addCreateTableAs(setting, "select col1 `col1`, (SELECT max(col2) from t2) `col2` from t1;");
        Assert.assertEquals("select col1 `col1`, (SELECT max(col2) from t2) `col2` INTO @outputTable from t1;", result);
    }

    @Test
    public void addCreateTableAs5() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test that it accepts line breaks instead of spaces
        String result = SQL.addCreateTableAs(setting, "select col1 \nfrom\nt1");
        String expected = "select col1 INTO @outputTable \nfrom\nt1";
        Assert.assertEquals(
                expected.replaceAll("\\s", " ").replaceAll("\\s+", " "),
                result.replaceAll("\\s", " ").replaceAll("\\s+", " ")
        );
    }

    @Test
    public void addCreateTableAs6() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test user variables without escaping. This is important in mainsample creation.
        String result = SQL.addCreateTableAs(setting, "SELECT t1.@CLIENT_FROM_DATE FROM t1");
        String expected = "SELECT t1.@CLIENT_FROM_DATE INTO @outputTable FROM t1";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void addCreateTableAs7() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test that it can deal with union all. This is important in sampling of base table.
        String result = SQL.addCreateTableAs(setting, "SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t2");
        String expected = "SELECT c1 INTO @outputTable FROM t1 UNION ALL SELECT c1 FROM t2";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void addCreateTableAs8() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test that it can deal with union all in brackets
        String result = SQL.addCreateTableAs(setting, "(SELECT c1 FROM t1) UNION ALL (SELECT c1 FROM t2)");
        String expected = "(SELECT c1 INTO @outputTable FROM t1) UNION ALL (SELECT c1 FROM t2)";
        Assert.assertEquals(expected, result);
    }

}