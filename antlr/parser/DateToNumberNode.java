package parser;

import autogenerated.SQLParser;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

public class DateToNumberNode extends TerminalNodeImpl {

	private final String text;

	public DateToNumberNode(SQLParser.DatetonumberContext context, String pattern) {
		// Constructor
		super(context.getStart());

		String column = context.payload().getText();

		text = pattern.replace("@column", column);
	}

	@Override
	public String getText() {
		return text;
	}
}
