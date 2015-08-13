// Generated from /Users/jan/Documents/Git/PredictorFactory/PredictorFactory/antlr/parser/SQL.g4 by ANTLR 4.5.1
package autogenerated;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SQLParser}.
 */
public interface SQLListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(SQLParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(SQLParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#bracket}.
	 * @param ctx the parse tree
	 */
	void enterBracket(SQLParser.BracketContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#bracket}.
	 * @param ctx the parse tree
	 */
	void exitBracket(SQLParser.BracketContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#datediff}.
	 * @param ctx the parse tree
	 */
	void enterDatediff(SQLParser.DatediffContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#datediff}.
	 * @param ctx the parse tree
	 */
	void exitDatediff(SQLParser.DatediffContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#corr}.
	 * @param ctx the parse tree
	 */
	void enterCorr(SQLParser.CorrContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#corr}.
	 * @param ctx the parse tree
	 */
	void exitCorr(SQLParser.CorrContext ctx);
	/**
	 * Enter a parse tree produced by {@link SQLParser#payload}.
	 * @param ctx the parse tree
	 */
	void enterPayload(SQLParser.PayloadContext ctx);
	/**
	 * Exit a parse tree produced by {@link SQLParser#payload}.
	 * @param ctx the parse tree
	 */
	void exitPayload(SQLParser.PayloadContext ctx);
}