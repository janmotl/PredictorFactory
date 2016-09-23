package utility;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.pp.para.GFmtOpt;
import gudusoft.gsqlparser.pp.para.GFmtOptFactory;
import gudusoft.gsqlparser.pp.para.GOutputFmt;
import gudusoft.gsqlparser.pp.para.styleenums.TCaseOption;
import gudusoft.gsqlparser.pp.stmtformatter.FormatterFactory;


public class FormatSQL {

	// Converts SQL text to colorful HTML SQL
	public static String formatSQL(String sql)
	{
		if (sql == null) {
			return "";
		}

		sql = sql.replaceAll("@","_"); // This parser dislikes @. Hence, we replace it with something neutral.

		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvgeneric);
		sqlparser.sqltext = sql;

		sqlparser.tokenizeSqltext();
		sqlparser.getrawsqlstatements();
		sqlparser.checkSyntax();

		GFmtOpt option = GFmtOptFactory.newInstance();
		option.outputFmt = GOutputFmt.ofhtml;
		return FormatterFactory.pp(sqlparser, option);

//		int ret = sqlparser.parse();
//		if (ret == 0){
//			GFmtOpt option = GFmtOptFactory.newInstance();
//			option.outputFmt = GOutputFmt.ofhtml;
//			return FormatterFactory.pp(sqlparser, option);
//		}else{
//			System.out.println(sqlparser.getErrormessage());
//			return sql;
//		}
	}



	public static void main(String args[])
	{
		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvpostgresql);

		sqlparser.sqltext = "select cOl1, col2,sum(col3) from tAble1, table2 where col4 > col5 and col6= 1000";

		sqlparser.sqltext = "SELECT base1, mode() WITHIN GROUP (ORDER BY column3) " +
				"   FROM propagatedTable1 " +
				"   GROUP BY base1";

		int ret = 0;

		sqlparser.parse();

		sqlparser.tokenizeSqltext();
		sqlparser.getrawsqlstatements();
		sqlparser.checkSyntax();

		if (ret == 0){
			GFmtOpt option = GFmtOptFactory.newInstance();
			option.caseIdentifier = TCaseOption.CoNoChange;
			option.insertBlankLineInBatchSqls = true;
			option.wsPaddingParenthesesOfSubQuery = true;
			//option.outputFmt = GOutputFmt.ofrtf;
			String result = FormatterFactory.pp(sqlparser, option);
			System.out.println(result);
		}else{
			System.out.println(sqlparser.getErrormessage());
		}
	}

}