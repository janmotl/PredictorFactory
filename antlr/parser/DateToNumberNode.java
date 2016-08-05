package parser;

import autogenerated.SQLParser;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;


// THIS IS A ROUGH IMPLEMENTATION - ONLY CONSTRUCTOR AND GETTEXT IS GUARANTEED TO WORK AS EXPECTED.
public class DateToNumberNode implements TerminalNode{

	private final SQLParser.DatetonumberContext ctx;
	private final String text;

	public DateToNumberNode(SQLParser.DatetonumberContext ctx2, String pattern) {
		ctx = ctx2;

        String column = ctx.payload().getText();

		text = pattern.replace("@column", column);
	}

	@Override
	public String toStringTree() {
		return ctx.toStringTree();
	}
	
	@Override
	public Object getPayload() {
		return ctx.getPayload();
	}
	
	@Override
	public int getChildCount() {
		return 0;
	}
	
	@Override
	public Interval getSourceInterval() {
		return ctx.getSourceInterval();
	}
	
	@Override
	public String toStringTree(Parser parser) {
		return ctx.toStringTree();
	}
	
	@Override
	public String getText() {
		return text;
	}
	
	@Override
	public ParseTree getParent() {
		return ctx.getParent();
	}
	
	@Override
	public ParseTree getChild(int i) {
		return ctx.getChild(i);
	}
	
	@Override
	public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
		return ctx.accept(visitor);
	}
	
	@Override
	public Token getSymbol() {
		return null;
	}
}

