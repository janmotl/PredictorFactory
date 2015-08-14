package parser;


import autogenerated.SQLBaseListener;
import autogenerated.SQLParser;

// Implements a listener, which replaces the old token with the new token.
public class DateToNumberListener extends SQLBaseListener {

	// Global parameters
	private String pattern;

	// Constructor
	public DateToNumberListener(String pattern) {
		this.pattern = pattern;
	}

	@Override
    public void enterDatetonumber(SQLParser.DatetonumberContext ctx) {
        
    	// Assembly a new token based on the data
    	DateToNumberToken newDateDiff = new DateToNumberToken(ctx, pattern);
    	
    	// Remove the old block
    	ctx.removeLastChild();
    	ctx.removeLastChild();
    	ctx.removeLastChild();
    	ctx.removeLastChild();
    	ctx.removeLastChild();
    	ctx.removeLastChild();
    	
    	// And replace it with the new block
    	ctx.addChild(newDateDiff);
    }
}
