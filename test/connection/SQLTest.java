package connection;

import extraction.Predictor;
import meta.ForeignConstraint;
import meta.OutputTable;
import mother.PredictorMother;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import run.Setting;

import java.util.ArrayList;
import java.util.Set;

public   class SQLTest {

    private OutputTable table;

    @Before
    public void initialization() {
        utility.Logging.initialization();

        table = new OutputTable();
        table.originalName = "trans";
        table.name = "propagated_trans";
	    table.schemaName = "financial";
        table.propagationTable = "propagated_account";
        table.propagationForeignConstraint = new ForeignConstraint();
    }


    @Test
    public void propagateID_simple() {
        // Setting
        Setting setting = new Setting("MSSQL Jan", "financial");
        setting.quoteEntityOpen = "[";
        setting.quoteEntityClose = "]";

        // Parameters
        table.propagationForeignConstraint.column = new ArrayList<>();
        table.propagationForeignConstraint.fColumn = new ArrayList<>();
        table.propagationForeignConstraint.column.add("account_id");
        table.propagationForeignConstraint.fColumn.add("account_id");

        // Test
        String actual =  setting.dialect.propagateID(setting, table);
        String expected = "SELECT t1.[propagated_id1], t1.[propagated_date], t1.[propagated_target1], t1.[propagated_fold], t2.* INTO [predictor_factory].[propagated_trans] FROM [predictor_factory].[propagated_account] t1 INNER JOIN [financial].[trans] t2 ON t1.[account_id] = t2.[account_id]";
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
        table.propagationForeignConstraint.column = new ArrayList<>();
        table.propagationForeignConstraint.fColumn = new ArrayList<>();
        table.propagationForeignConstraint.column.add("account_id1");
        table.propagationForeignConstraint.fColumn.add("account_id2");
        table.propagationForeignConstraint.column.add("account_id3");
        table.propagationForeignConstraint.fColumn.add("account_id4");

        // Test
        String actual =  setting.dialect.propagateID(setting, table);
        String expected = "SELECT t1.[propagated_id1], t1.[propagated_id2], t1.[propagated_date], t1.[propagated_target1], t1.[propagated_fold], t2.* INTO [predictor_factory].[propagated_trans] FROM [predictor_factory].[propagated_account] t1 INNER JOIN [financial].[trans] t2 ON t1.[account_id1] = t2.[account_id2] AND t1.[account_id3] = t2.[account_id4]";
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void addCreateTableAs1() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test user variable compliance (tolerance to at-sign). Important as I am using at-signs in patterns.
        String result = setting.dialect.addCreateTableAs(setting, "SELECT @column FROM table");
        Assert.assertEquals("SELECT @column INTO @outputTable FROM table", result);
    }

    @Test
    public void addCreateTableAs2() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test ignorance of a table called "from". This is important in the maintable assembly.
        String result = setting.dialect.addCreateTableAs(setting, "SELECT [from] FROM table");
        Assert.assertEquals("SELECT [from] INTO @outputTable FROM table", result);
    }

    @Test
    public void addCreateTableAs3() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test for case insensitivity. As the SQL Keywords are case-insensitive in SQL 92.
        String result = setting.dialect.addCreateTableAs(setting, "SELECT col FrOm table");
        String expected = "SELECT col INTO @outputTable FrOm table";
        Assert.assertEquals(expected.toLowerCase(), result.toLowerCase());
    }

    @Test
    public void addCreateTableAs4() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test with subquery in the select part.
        String result = setting.dialect.addCreateTableAs(setting, "select col1 `col1`, (SELECT max(col2) from t2) `col2` from t1;");
        Assert.assertEquals("select col1 `col1`, (SELECT max(col2) from t2) `col2` INTO @outputTable from t1;", result);
    }

    @Test
    public void addCreateTableAs5() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test that it accepts line breaks instead of spaces
        String result = setting.dialect.addCreateTableAs(setting, "select col1 \nfrom\nt1");
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
        String result = setting.dialect.addCreateTableAs(setting, "SELECT t1.@CLIENT_FROM_DATE FROM t1");
        String expected = "SELECT t1.@CLIENT_FROM_DATE INTO @outputTable FROM t1";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void addCreateTableAs7() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test that it can deal with union all. This is important in sampling of base table.
        String result = setting.dialect.addCreateTableAs(setting, "SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t2");
        String expected = "SELECT c1 INTO @outputTable FROM t1 UNION ALL SELECT c1 FROM t2";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void addCreateTableAs8() {
        // Setting
        Setting setting = new Setting();
        setting.supportsCreateTableAs = false;

        // Test that it can deal with union all in brackets
        String result = setting.dialect.addCreateTableAs(setting, "(SELECT c1 FROM t1) UNION ALL (SELECT c1 FROM t2)");
        String expected = "(SELECT c1 INTO @outputTable FROM t1) UNION ALL (SELECT c1 FROM t2)";
        Assert.assertEquals(expected, result);
    }

    @Test
    public void containsNullPostgreSQL() {
        // Setting
        Setting setting = new Setting("PostgreSQL", "financial");
        Network.openConnection(setting);
        setting.lag = 3;
        setting.lead = 0;
        setting.unit = "month";

		// No missing values
        boolean result = setting.dialect.containsNull(setting, "financial", "trans", "date");
        Assert.assertFalse(result);

		// Missing values
		boolean result2 = setting.dialect.containsNull(setting, "financial", "trans", "k_symbol");
		Assert.assertTrue(result2);

	    Network.closeConnection(setting);
    }

	@Test
	public void getTopUniqueRecordsPostgreSQL() {
		// Setting
		Setting setting = new Setting("PostgreSQL", "financial");
		setting.valueCount = 4;
		Network.openConnection(setting);

		Set<String> records = setting.dialect.getTopUniqueRecords(setting, "financial", "district", "A3").keySet();
		Assert.assertEquals(setting.valueCount, records.size());    // The size is upper bounded (from 8 to 4)
		Assert.assertTrue(records.contains("south Moravia"));

		Network.closeConnection(setting);
	}

    @Test
    public void addJournalRunOracle() {
        // Setting
        Setting setting = new Setting("Oracle", "financial_xe");
        Network.openConnection(setting);
        //setting.dialect.getJournalRun(setting);   // Create journal_run

        setting.dialect.addJournalRun(setting, 100L); // Assumes journal_run is already there

        Assert.assertTrue(utility.CountAppender.getCount(Level.WARN) == 0);

        Network.closeConnection(setting);
    }

    @Test
    public void getChi2() {
        // Design: unless stated otherwise, we test columns without any missing value (to make it comparable to RapidMiner)

        // Setting
        Setting setting = new Setting("MariaDB", "financial");
	    setting.outputSchema = "financial";     // We use input schemas instead of predictor_factory
        Network.openConnection(setting);
		Predictor predictor = PredictorMother.aggregateAvg();

	    // Char
	    predictor.setOutputTable("order");
		predictor.setName("k_symbol");
	    predictor.setDataTypeCategory("nominal");
	    double chi2 = 5*SQL.getChi2(setting, predictor, "bank_to"); // k_symbol has 5 categories
        Assert.assertEquals(51.31649200007232, chi2, 0.0001); // Measured against RapidMiner

	    // Integer with a few distinct values
	    predictor.setOutputTable("loan");
		predictor.setName("duration");
	    predictor.setDataTypeCategory("numerical");
	    chi2 = 10*SQL.getChi2(setting, predictor, "status"); // Binning into 10 bins (regardless of the unique count)
        Assert.assertEquals(246.51818703480066, chi2, 0.01); // Measured against RapidMiner
	    // The calculator at http://www.quantpsy.org/chisq/chisq.htm says: 246.518
	    // Conclusion: Some small error is present.

	    // Integer with many distinct values, some empirical counts are zero
	    predictor.setOutputTable("loan");
		predictor.setName("amount");
	    predictor.setDataTypeCategory("numerical");
	    chi2 = 10*SQL.getChi2(setting, predictor, "status"); // Binning into 10 bins
        Assert.assertEquals(125.2037214042268, chi2, 0.01); // RapidMiner is also using equal width binning, the histograms exactly match -> have to match
	    // The calculator at http://www.quantpsy.org/chisq/chisq.htm says: 125.204

	    // Date
	    predictor.setOutputTable("loan");
		predictor.setName("date");
	    predictor.setDataTypeCategory("temporal");
	    chi2 = 10*SQL.getChi2(setting, predictor, "status"); // Binning into 10 bins
        Assert.assertEquals(274.75594924317306, chi2, 0.01); // RapidMiner reference

        // Time
	    setting.outputSchema = "geneea";
	    predictor.setOutputTable("hl_hlasovani");
		predictor.setName("cas");
	    predictor.setDataTypeCategory("temporal");
	    chi2 = 10*SQL.getChi2(setting, predictor, "vysledek"); // Binning into 10 bins
        Assert.assertEquals(58.205060470034205, chi2, 0.001); // RapidMiner reference

	    // Timestamp
	    setting.outputSchema = "AdventureWorks2014";
	    predictor.setOutputTable("EmployeePayHistory");
		predictor.setName("ModifiedDate");
	    predictor.setDataTypeCategory("temporal");
	    chi2 = 10*SQL.getChi2(setting, predictor, "PayFrequency"); // Binning into 10 bins
        Assert.assertEquals(21.856300708852554, chi2, 0.001); // RapidMiner reference

        // Text (slow and with nulls -> find better test)
        setting.outputSchema = "FNHK";
        predictor.setOutputTable("pripady");
        predictor.setName("PSC");
        predictor.setDataTypeCategory("nominal");
        chi2 = 1202*SQL.getChi2(setting, predictor, "Pohlavi_pacienta"); // 1202 unique values in PSC
        Assert.assertEquals(1931.1719247408, chi2, 0.01);
        // R with double precision:       1931.17192474082753 without nulls (excluded from the calculation)
        // R 140 bit precision:           1931.17192474082751 without nulls
        // RapidMiner reference:          1931.1719247408496  without nulls
        // Databases numeric precision:   1931.1719247408275  without nulls -> without nulls correct
        // ------
        // R with double precision:       1931.54616075496073 with nulls (included in the raw data)
        // R 140 bit precision:           1931.54616075496065 with nulls
        // RapidMiner reference:          1930.9136306749358  with nulls
        // Databases numeric precision:   1934.16990049500675 with nulls
        // Databases float8 precision:    1934.16990049502    with nulls -> off by 0.001
        // Databases float4 precision:    1934.17             with nulls
        //
        // When comparing to R, we should remove the correction, which affects the statistics.
        // When calculating from a contingency table:
        //      M <- as.table(rbind(c(1, 0), c(0, 1)))
        //      print(chisq.test(M, correct=FALSE))     # We expect to see value 2
        // When calculating from raw data:
        //      tbl = table(input$Pohlavi_pacienta, input$PSC)
        //      print(chisq.test(tbl, correct=FALSE))


	    // Enum, blob, binary, text


        // Nulls numerical (see the implementation for explanation why the results differ )
        setting.outputSchema = "pubs";
	    predictor.setOutputTable("titles");
		predictor.setName("royalty");
	    predictor.setDataTypeCategory("numerical");
	    chi2 = 10*SQL.getChi2(setting, predictor, "type"); // It is always divided by 10 for numerical
        Assert.assertEquals(20.5866666666666, chi2, 2);
        // http://www.quantpsy.org/chisq/chisq.htm (we treat null as another category): 31.86
        // http://www.quantpsy.org/chisq/chisq.htm (we remove whole rows with nulls): 20.587
        // Rapidminer: 20.724999999999998


        // Nulls nominal
        setting.outputSchema = "pubs";
	    predictor.setOutputTable("titles");
		predictor.setName("royalty");
	    predictor.setDataTypeCategory("nominal");
	    chi2 = 5*SQL.getChi2(setting, predictor, "type"); // 6 unique values in the label, but one always has null feature
        Assert.assertEquals(20.5866666666666, chi2, 2);


        // Nulls nominal
        // The convention is to remove records with missing values:
        //      http://stat.ethz.ch/R-manual/R-patched/library/stats/html/chisq.test.html
        setting.outputSchema = "pubs";
	    predictor.setOutputTable("publishers");
		predictor.setName("state");
	    predictor.setDataTypeCategory("nominal");
	    chi2 = 6*SQL.getChi2(setting, predictor, "country"); // 6+1 unique values in the state
        Assert.assertEquals(2, chi2, 0.1);
        // http://www.quantpsy.org/chisq/chisq.htm (we treat null as another category): 8
        // R (we treat null as a valid value, not as NA): 8
        // R (we treat null as NA): NaN
        // RapidMiner: 4.444444444444445






        // Constant feature

        Network.closeConnection(setting);
    }

	@Test
    public void getR2() {
        // Setting
        Setting setting = new Setting("MariaDB", "Biodegradability");
	    setting.outputSchema = "Biodegradability";
        Network.openConnection(setting);
		Predictor predictor = PredictorMother.aggregateAvg();

	    // Numeric
	    predictor.setOutputTable("molecule");
		predictor.setName("logp");
	    predictor.setDataTypeCategory("numerical");
	    double chi2 = SQL.getR2(setting, predictor, "activity");
        Assert.assertEquals(68.4447497804554, chi2, 0.0001); // Measured against PostgreSQL: SELECT corr(activity, logp)^2 * count(*)

        Network.closeConnection(setting);
    }
}