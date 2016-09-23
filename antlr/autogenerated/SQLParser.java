// Generated from /Users/jan/Documents/Git/PredictorFactory/antlr/parser/SQL.g4 by ANTLR 4.5.3
package autogenerated;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		FROM=1, JOIN=2, ON=3, AND=4, USING=5, DATEDIFF=6, DATETONUMBER=7, CORR=8, 
		LBR=9, RBR=10, COMMA=11, EQUALS=12, ARITHMETIC=13, TEXT=14, WS=15;
	public static final int
		RULE_expression = 0, RULE_bracket = 1, RULE_datediff = 2, RULE_datetonumber = 3, 
		RULE_corr = 4, RULE_from = 5, RULE_table = 6, RULE_alias = 7, RULE_using = 8, 
		RULE_columns = 9, RULE_on = 10, RULE_payload = 11;
	public static final String[] ruleNames = {
		"expression", "bracket", "datediff", "datetonumber", "corr", "from", "table", 
		"alias", "using", "columns", "on", "payload"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, "'('", "')'", "','", 
		"'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "FROM", "JOIN", "ON", "AND", "USING", "DATEDIFF", "DATETONUMBER", 
		"CORR", "LBR", "RBR", "COMMA", "EQUALS", "ARITHMETIC", "TEXT", "WS"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class ExpressionContext extends ParserRuleContext {
		public List<FromContext> from() {
			return getRuleContexts(FromContext.class);
		}
		public FromContext from(int i) {
			return getRuleContext(FromContext.class,i);
		}
		public List<TerminalNode> TEXT() { return getTokens(SQLParser.TEXT); }
		public TerminalNode TEXT(int i) {
			return getToken(SQLParser.TEXT, i);
		}
		public List<BracketContext> bracket() {
			return getRuleContexts(BracketContext.class);
		}
		public BracketContext bracket(int i) {
			return getRuleContext(BracketContext.class,i);
		}
		public List<TerminalNode> WS() { return getTokens(SQLParser.WS); }
		public TerminalNode WS(int i) {
			return getToken(SQLParser.WS, i);
		}
		public List<TerminalNode> AND() { return getTokens(SQLParser.AND); }
		public TerminalNode AND(int i) {
			return getToken(SQLParser.AND, i);
		}
		public List<TerminalNode> EQUALS() { return getTokens(SQLParser.EQUALS); }
		public TerminalNode EQUALS(int i) {
			return getToken(SQLParser.EQUALS, i);
		}
		public List<TerminalNode> ARITHMETIC() { return getTokens(SQLParser.ARITHMETIC); }
		public TerminalNode ARITHMETIC(int i) {
			return getToken(SQLParser.ARITHMETIC, i);
		}
		public List<DatediffContext> datediff() {
			return getRuleContexts(DatediffContext.class);
		}
		public DatediffContext datediff(int i) {
			return getRuleContext(DatediffContext.class,i);
		}
		public List<DatetonumberContext> datetonumber() {
			return getRuleContexts(DatetonumberContext.class);
		}
		public DatetonumberContext datetonumber(int i) {
			return getRuleContext(DatetonumberContext.class,i);
		}
		public List<CorrContext> corr() {
			return getRuleContexts(CorrContext.class);
		}
		public CorrContext corr(int i) {
			return getRuleContext(CorrContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLParser.COMMA, i);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitExpression(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_expression);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(37);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,1,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(35);
					switch (_input.LA(1)) {
					case FROM:
						{
						setState(24);
						from();
						}
						break;
					case TEXT:
						{
						setState(25);
						match(TEXT);
						}
						break;
					case LBR:
						{
						setState(26);
						bracket();
						}
						break;
					case WS:
						{
						setState(27);
						match(WS);
						}
						break;
					case AND:
						{
						setState(28);
						match(AND);
						}
						break;
					case EQUALS:
						{
						setState(29);
						match(EQUALS);
						}
						break;
					case ARITHMETIC:
						{
						setState(30);
						match(ARITHMETIC);
						}
						break;
					case DATEDIFF:
						{
						setState(31);
						datediff();
						}
						break;
					case DATETONUMBER:
						{
						setState(32);
						datetonumber();
						}
						break;
					case CORR:
						{
						setState(33);
						corr();
						}
						break;
					case COMMA:
						{
						setState(34);
						match(COMMA);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					} 
				}
				setState(39);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,1,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BracketContext extends ParserRuleContext {
		public TerminalNode LBR() { return getToken(SQLParser.LBR, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode RBR() { return getToken(SQLParser.RBR, 0); }
		public BracketContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bracket; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterBracket(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitBracket(this);
		}
	}

	public final BracketContext bracket() throws RecognitionException {
		BracketContext _localctx = new BracketContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_bracket);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(40);
			match(LBR);
			setState(41);
			expression();
			setState(42);
			match(RBR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatediffContext extends ParserRuleContext {
		public TerminalNode DATEDIFF() { return getToken(SQLParser.DATEDIFF, 0); }
		public TerminalNode LBR() { return getToken(SQLParser.LBR, 0); }
		public List<PayloadContext> payload() {
			return getRuleContexts(PayloadContext.class);
		}
		public PayloadContext payload(int i) {
			return getRuleContext(PayloadContext.class,i);
		}
		public TerminalNode COMMA() { return getToken(SQLParser.COMMA, 0); }
		public TerminalNode RBR() { return getToken(SQLParser.RBR, 0); }
		public DatediffContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datediff; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDatediff(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDatediff(this);
		}
	}

	public final DatediffContext datediff() throws RecognitionException {
		DatediffContext _localctx = new DatediffContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_datediff);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(44);
			match(DATEDIFF);
			setState(45);
			match(LBR);
			setState(46);
			payload();
			setState(47);
			match(COMMA);
			setState(48);
			payload();
			setState(49);
			match(RBR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatetonumberContext extends ParserRuleContext {
		public TerminalNode DATETONUMBER() { return getToken(SQLParser.DATETONUMBER, 0); }
		public TerminalNode LBR() { return getToken(SQLParser.LBR, 0); }
		public PayloadContext payload() {
			return getRuleContext(PayloadContext.class,0);
		}
		public TerminalNode RBR() { return getToken(SQLParser.RBR, 0); }
		public DatetonumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datetonumber; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterDatetonumber(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitDatetonumber(this);
		}
	}

	public final DatetonumberContext datetonumber() throws RecognitionException {
		DatetonumberContext _localctx = new DatetonumberContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_datetonumber);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(51);
			match(DATETONUMBER);
			setState(52);
			match(LBR);
			setState(53);
			payload();
			setState(54);
			match(RBR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CorrContext extends ParserRuleContext {
		public TerminalNode CORR() { return getToken(SQLParser.CORR, 0); }
		public TerminalNode LBR() { return getToken(SQLParser.LBR, 0); }
		public List<PayloadContext> payload() {
			return getRuleContexts(PayloadContext.class);
		}
		public PayloadContext payload(int i) {
			return getRuleContext(PayloadContext.class,i);
		}
		public TerminalNode COMMA() { return getToken(SQLParser.COMMA, 0); }
		public TerminalNode RBR() { return getToken(SQLParser.RBR, 0); }
		public CorrContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_corr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterCorr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitCorr(this);
		}
	}

	public final CorrContext corr() throws RecognitionException {
		CorrContext _localctx = new CorrContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_corr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(56);
			match(CORR);
			setState(57);
			match(LBR);
			setState(58);
			payload();
			setState(59);
			match(COMMA);
			setState(60);
			payload();
			setState(61);
			match(RBR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SQLParser.FROM, 0); }
		public List<BracketContext> bracket() {
			return getRuleContexts(BracketContext.class);
		}
		public BracketContext bracket(int i) {
			return getRuleContext(BracketContext.class,i);
		}
		public List<TableContext> table() {
			return getRuleContexts(TableContext.class);
		}
		public TableContext table(int i) {
			return getRuleContext(TableContext.class,i);
		}
		public List<TerminalNode> WS() { return getTokens(SQLParser.WS); }
		public TerminalNode WS(int i) {
			return getToken(SQLParser.WS, i);
		}
		public List<AliasContext> alias() {
			return getRuleContexts(AliasContext.class);
		}
		public AliasContext alias(int i) {
			return getRuleContext(AliasContext.class,i);
		}
		public List<TerminalNode> JOIN() { return getTokens(SQLParser.JOIN); }
		public TerminalNode JOIN(int i) {
			return getToken(SQLParser.JOIN, i);
		}
		public List<TerminalNode> TEXT() { return getTokens(SQLParser.TEXT); }
		public TerminalNode TEXT(int i) {
			return getToken(SQLParser.TEXT, i);
		}
		public List<UsingContext> using() {
			return getRuleContexts(UsingContext.class);
		}
		public UsingContext using(int i) {
			return getRuleContext(UsingContext.class,i);
		}
		public List<OnContext> on() {
			return getRuleContexts(OnContext.class);
		}
		public OnContext on(int i) {
			return getRuleContext(OnContext.class,i);
		}
		public FromContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_from; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterFrom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitFrom(this);
		}
	}

	public final FromContext from() throws RecognitionException {
		FromContext _localctx = new FromContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_from);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(63);
			match(FROM);
			setState(67);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==WS) {
				{
				{
				setState(64);
				match(WS);
				}
				}
				setState(69);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(72);
			switch (_input.LA(1)) {
			case LBR:
				{
				setState(70);
				bracket();
				}
				break;
			case TEXT:
				{
				setState(71);
				table();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(77);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(74);
					match(WS);
					}
					} 
				}
				setState(79);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,4,_ctx);
			}
			setState(81);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				{
				setState(80);
				alias();
				}
				break;
			}
			setState(145);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(86);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(83);
							match(WS);
							}
							} 
						}
						setState(88);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
					}
					setState(92);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(89);
							match(TEXT);
							}
							} 
						}
						setState(94);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
					}
					setState(98);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(95);
							match(WS);
							}
							} 
						}
						setState(100);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
					}
					setState(104);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==TEXT) {
						{
						{
						setState(101);
						match(TEXT);
						}
						}
						setState(106);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(110);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==WS) {
						{
						{
						setState(107);
						match(WS);
						}
						}
						setState(112);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(113);
					match(JOIN);
					setState(117);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==WS) {
						{
						{
						setState(114);
						match(WS);
						}
						}
						setState(119);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(122);
					switch (_input.LA(1)) {
					case LBR:
						{
						setState(120);
						bracket();
						}
						break;
					case TEXT:
						{
						setState(121);
						table();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(127);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(124);
							match(WS);
							}
							} 
						}
						setState(129);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
					}
					setState(131);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
					case 1:
						{
						setState(130);
						alias();
						}
						break;
					}
					setState(136);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
					while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
						if ( _alt==1 ) {
							{
							{
							setState(133);
							match(WS);
							}
							} 
						}
						setState(138);
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,15,_ctx);
					}
					setState(141);
					switch (_input.LA(1)) {
					case USING:
						{
						setState(139);
						using();
						}
						break;
					case ON:
						{
						setState(140);
						on();
						}
						break;
					case FROM:
					case JOIN:
					case AND:
					case DATEDIFF:
					case DATETONUMBER:
					case CORR:
					case LBR:
					case RBR:
					case COMMA:
					case EQUALS:
					case ARITHMETIC:
					case TEXT:
					case WS:
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
					} 
				}
				setState(147);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,17,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableContext extends ParserRuleContext {
		public TerminalNode TEXT() { return getToken(SQLParser.TEXT, 0); }
		public TableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitTable(this);
		}
	}

	public final TableContext table() throws RecognitionException {
		TableContext _localctx = new TableContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_table);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(148);
			match(TEXT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AliasContext extends ParserRuleContext {
		public TerminalNode TEXT() { return getToken(SQLParser.TEXT, 0); }
		public AliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitAlias(this);
		}
	}

	public final AliasContext alias() throws RecognitionException {
		AliasContext _localctx = new AliasContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(150);
			match(TEXT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UsingContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SQLParser.USING, 0); }
		public TerminalNode LBR() { return getToken(SQLParser.LBR, 0); }
		public ColumnsContext columns() {
			return getRuleContext(ColumnsContext.class,0);
		}
		public TerminalNode RBR() { return getToken(SQLParser.RBR, 0); }
		public List<TerminalNode> WS() { return getTokens(SQLParser.WS); }
		public TerminalNode WS(int i) {
			return getToken(SQLParser.WS, i);
		}
		public UsingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_using; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitUsing(this);
		}
	}

	public final UsingContext using() throws RecognitionException {
		UsingContext _localctx = new UsingContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_using);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(152);
			match(USING);
			setState(156);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==WS) {
				{
				{
				setState(153);
				match(WS);
				}
				}
				setState(158);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(159);
			match(LBR);
			setState(163);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==WS) {
				{
				{
				setState(160);
				match(WS);
				}
				}
				setState(165);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(166);
			columns();
			setState(170);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==WS) {
				{
				{
				setState(167);
				match(WS);
				}
				}
				setState(172);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(173);
			match(RBR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnsContext extends ParserRuleContext {
		public List<TerminalNode> TEXT() { return getTokens(SQLParser.TEXT); }
		public TerminalNode TEXT(int i) {
			return getToken(SQLParser.TEXT, i);
		}
		public List<TerminalNode> COMMA() { return getTokens(SQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(SQLParser.COMMA, i);
		}
		public List<TerminalNode> WS() { return getTokens(SQLParser.WS); }
		public TerminalNode WS(int i) {
			return getToken(SQLParser.WS, i);
		}
		public ColumnsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columns; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitColumns(this);
		}
	}

	public final ColumnsContext columns() throws RecognitionException {
		ColumnsContext _localctx = new ColumnsContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_columns);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(175);
			match(TEXT);
			setState(192);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(179);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==WS) {
						{
						{
						setState(176);
						match(WS);
						}
						}
						setState(181);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(182);
					match(COMMA);
					setState(186);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==WS) {
						{
						{
						setState(183);
						match(WS);
						}
						}
						setState(188);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(189);
					match(TEXT);
					}
					} 
				}
				setState(194);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,23,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OnContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SQLParser.ON, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public OnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_on; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterOn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitOn(this);
		}
	}

	public final OnContext on() throws RecognitionException {
		OnContext _localctx = new OnContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_on);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(195);
			match(ON);
			setState(196);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PayloadContext extends ParserRuleContext {
		public List<FromContext> from() {
			return getRuleContexts(FromContext.class);
		}
		public FromContext from(int i) {
			return getRuleContext(FromContext.class,i);
		}
		public List<TerminalNode> TEXT() { return getTokens(SQLParser.TEXT); }
		public TerminalNode TEXT(int i) {
			return getToken(SQLParser.TEXT, i);
		}
		public List<BracketContext> bracket() {
			return getRuleContexts(BracketContext.class);
		}
		public BracketContext bracket(int i) {
			return getRuleContext(BracketContext.class,i);
		}
		public List<TerminalNode> WS() { return getTokens(SQLParser.WS); }
		public TerminalNode WS(int i) {
			return getToken(SQLParser.WS, i);
		}
		public List<TerminalNode> AND() { return getTokens(SQLParser.AND); }
		public TerminalNode AND(int i) {
			return getToken(SQLParser.AND, i);
		}
		public List<TerminalNode> EQUALS() { return getTokens(SQLParser.EQUALS); }
		public TerminalNode EQUALS(int i) {
			return getToken(SQLParser.EQUALS, i);
		}
		public List<TerminalNode> ARITHMETIC() { return getTokens(SQLParser.ARITHMETIC); }
		public TerminalNode ARITHMETIC(int i) {
			return getToken(SQLParser.ARITHMETIC, i);
		}
		public List<DatediffContext> datediff() {
			return getRuleContexts(DatediffContext.class);
		}
		public DatediffContext datediff(int i) {
			return getRuleContext(DatediffContext.class,i);
		}
		public List<DatetonumberContext> datetonumber() {
			return getRuleContexts(DatetonumberContext.class);
		}
		public DatetonumberContext datetonumber(int i) {
			return getRuleContext(DatetonumberContext.class,i);
		}
		public List<CorrContext> corr() {
			return getRuleContexts(CorrContext.class);
		}
		public CorrContext corr(int i) {
			return getRuleContext(CorrContext.class,i);
		}
		public PayloadContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_payload; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).enterPayload(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SQLListener ) ((SQLListener)listener).exitPayload(this);
		}
	}

	public final PayloadContext payload() throws RecognitionException {
		PayloadContext _localctx = new PayloadContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_payload);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(210);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << FROM) | (1L << AND) | (1L << DATEDIFF) | (1L << DATETONUMBER) | (1L << CORR) | (1L << LBR) | (1L << EQUALS) | (1L << ARITHMETIC) | (1L << TEXT) | (1L << WS))) != 0)) {
				{
				setState(208);
				switch (_input.LA(1)) {
				case FROM:
					{
					setState(198);
					from();
					}
					break;
				case TEXT:
					{
					setState(199);
					match(TEXT);
					}
					break;
				case LBR:
					{
					setState(200);
					bracket();
					}
					break;
				case WS:
					{
					setState(201);
					match(WS);
					}
					break;
				case AND:
					{
					setState(202);
					match(AND);
					}
					break;
				case EQUALS:
					{
					setState(203);
					match(EQUALS);
					}
					break;
				case ARITHMETIC:
					{
					setState(204);
					match(ARITHMETIC);
					}
					break;
				case DATEDIFF:
					{
					setState(205);
					datediff();
					}
					break;
				case DATETONUMBER:
					{
					setState(206);
					datetonumber();
					}
					break;
				case CORR:
					{
					setState(207);
					corr();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(212);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\21\u00d8\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\7\2"+
		"&\n\2\f\2\16\2)\13\2\3\3\3\3\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\5\3"+
		"\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\7\3\7\7\7D\n\7\f\7\16\7G"+
		"\13\7\3\7\3\7\5\7K\n\7\3\7\7\7N\n\7\f\7\16\7Q\13\7\3\7\5\7T\n\7\3\7\7"+
		"\7W\n\7\f\7\16\7Z\13\7\3\7\7\7]\n\7\f\7\16\7`\13\7\3\7\7\7c\n\7\f\7\16"+
		"\7f\13\7\3\7\7\7i\n\7\f\7\16\7l\13\7\3\7\7\7o\n\7\f\7\16\7r\13\7\3\7\3"+
		"\7\7\7v\n\7\f\7\16\7y\13\7\3\7\3\7\5\7}\n\7\3\7\7\7\u0080\n\7\f\7\16\7"+
		"\u0083\13\7\3\7\5\7\u0086\n\7\3\7\7\7\u0089\n\7\f\7\16\7\u008c\13\7\3"+
		"\7\3\7\5\7\u0090\n\7\7\7\u0092\n\7\f\7\16\7\u0095\13\7\3\b\3\b\3\t\3\t"+
		"\3\n\3\n\7\n\u009d\n\n\f\n\16\n\u00a0\13\n\3\n\3\n\7\n\u00a4\n\n\f\n\16"+
		"\n\u00a7\13\n\3\n\3\n\7\n\u00ab\n\n\f\n\16\n\u00ae\13\n\3\n\3\n\3\13\3"+
		"\13\7\13\u00b4\n\13\f\13\16\13\u00b7\13\13\3\13\3\13\7\13\u00bb\n\13\f"+
		"\13\16\13\u00be\13\13\3\13\7\13\u00c1\n\13\f\13\16\13\u00c4\13\13\3\f"+
		"\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\7\r\u00d3\n\r\f\r\16"+
		"\r\u00d6\13\r\3\r\2\2\16\2\4\6\b\n\f\16\20\22\24\26\30\2\2\u00f7\2\'\3"+
		"\2\2\2\4*\3\2\2\2\6.\3\2\2\2\b\65\3\2\2\2\n:\3\2\2\2\fA\3\2\2\2\16\u0096"+
		"\3\2\2\2\20\u0098\3\2\2\2\22\u009a\3\2\2\2\24\u00b1\3\2\2\2\26\u00c5\3"+
		"\2\2\2\30\u00d4\3\2\2\2\32&\5\f\7\2\33&\7\20\2\2\34&\5\4\3\2\35&\7\21"+
		"\2\2\36&\7\6\2\2\37&\7\16\2\2 &\7\17\2\2!&\5\6\4\2\"&\5\b\5\2#&\5\n\6"+
		"\2$&\7\r\2\2%\32\3\2\2\2%\33\3\2\2\2%\34\3\2\2\2%\35\3\2\2\2%\36\3\2\2"+
		"\2%\37\3\2\2\2% \3\2\2\2%!\3\2\2\2%\"\3\2\2\2%#\3\2\2\2%$\3\2\2\2&)\3"+
		"\2\2\2\'%\3\2\2\2\'(\3\2\2\2(\3\3\2\2\2)\'\3\2\2\2*+\7\13\2\2+,\5\2\2"+
		"\2,-\7\f\2\2-\5\3\2\2\2./\7\b\2\2/\60\7\13\2\2\60\61\5\30\r\2\61\62\7"+
		"\r\2\2\62\63\5\30\r\2\63\64\7\f\2\2\64\7\3\2\2\2\65\66\7\t\2\2\66\67\7"+
		"\13\2\2\678\5\30\r\289\7\f\2\29\t\3\2\2\2:;\7\n\2\2;<\7\13\2\2<=\5\30"+
		"\r\2=>\7\r\2\2>?\5\30\r\2?@\7\f\2\2@\13\3\2\2\2AE\7\3\2\2BD\7\21\2\2C"+
		"B\3\2\2\2DG\3\2\2\2EC\3\2\2\2EF\3\2\2\2FJ\3\2\2\2GE\3\2\2\2HK\5\4\3\2"+
		"IK\5\16\b\2JH\3\2\2\2JI\3\2\2\2KO\3\2\2\2LN\7\21\2\2ML\3\2\2\2NQ\3\2\2"+
		"\2OM\3\2\2\2OP\3\2\2\2PS\3\2\2\2QO\3\2\2\2RT\5\20\t\2SR\3\2\2\2ST\3\2"+
		"\2\2T\u0093\3\2\2\2UW\7\21\2\2VU\3\2\2\2WZ\3\2\2\2XV\3\2\2\2XY\3\2\2\2"+
		"Y^\3\2\2\2ZX\3\2\2\2[]\7\20\2\2\\[\3\2\2\2]`\3\2\2\2^\\\3\2\2\2^_\3\2"+
		"\2\2_d\3\2\2\2`^\3\2\2\2ac\7\21\2\2ba\3\2\2\2cf\3\2\2\2db\3\2\2\2de\3"+
		"\2\2\2ej\3\2\2\2fd\3\2\2\2gi\7\20\2\2hg\3\2\2\2il\3\2\2\2jh\3\2\2\2jk"+
		"\3\2\2\2kp\3\2\2\2lj\3\2\2\2mo\7\21\2\2nm\3\2\2\2or\3\2\2\2pn\3\2\2\2"+
		"pq\3\2\2\2qs\3\2\2\2rp\3\2\2\2sw\7\4\2\2tv\7\21\2\2ut\3\2\2\2vy\3\2\2"+
		"\2wu\3\2\2\2wx\3\2\2\2x|\3\2\2\2yw\3\2\2\2z}\5\4\3\2{}\5\16\b\2|z\3\2"+
		"\2\2|{\3\2\2\2}\u0081\3\2\2\2~\u0080\7\21\2\2\177~\3\2\2\2\u0080\u0083"+
		"\3\2\2\2\u0081\177\3\2\2\2\u0081\u0082\3\2\2\2\u0082\u0085\3\2\2\2\u0083"+
		"\u0081\3\2\2\2\u0084\u0086\5\20\t\2\u0085\u0084\3\2\2\2\u0085\u0086\3"+
		"\2\2\2\u0086\u008a\3\2\2\2\u0087\u0089\7\21\2\2\u0088\u0087\3\2\2\2\u0089"+
		"\u008c\3\2\2\2\u008a\u0088\3\2\2\2\u008a\u008b\3\2\2\2\u008b\u008f\3\2"+
		"\2\2\u008c\u008a\3\2\2\2\u008d\u0090\5\22\n\2\u008e\u0090\5\26\f\2\u008f"+
		"\u008d\3\2\2\2\u008f\u008e\3\2\2\2\u008f\u0090\3\2\2\2\u0090\u0092\3\2"+
		"\2\2\u0091X\3\2\2\2\u0092\u0095\3\2\2\2\u0093\u0091\3\2\2\2\u0093\u0094"+
		"\3\2\2\2\u0094\r\3\2\2\2\u0095\u0093\3\2\2\2\u0096\u0097\7\20\2\2\u0097"+
		"\17\3\2\2\2\u0098\u0099\7\20\2\2\u0099\21\3\2\2\2\u009a\u009e\7\7\2\2"+
		"\u009b\u009d\7\21\2\2\u009c\u009b\3\2\2\2\u009d\u00a0\3\2\2\2\u009e\u009c"+
		"\3\2\2\2\u009e\u009f\3\2\2\2\u009f\u00a1\3\2\2\2\u00a0\u009e\3\2\2\2\u00a1"+
		"\u00a5\7\13\2\2\u00a2\u00a4\7\21\2\2\u00a3\u00a2\3\2\2\2\u00a4\u00a7\3"+
		"\2\2\2\u00a5\u00a3\3\2\2\2\u00a5\u00a6\3\2\2\2\u00a6\u00a8\3\2\2\2\u00a7"+
		"\u00a5\3\2\2\2\u00a8\u00ac\5\24\13\2\u00a9\u00ab\7\21\2\2\u00aa\u00a9"+
		"\3\2\2\2\u00ab\u00ae\3\2\2\2\u00ac\u00aa\3\2\2\2\u00ac\u00ad\3\2\2\2\u00ad"+
		"\u00af\3\2\2\2\u00ae\u00ac\3\2\2\2\u00af\u00b0\7\f\2\2\u00b0\23\3\2\2"+
		"\2\u00b1\u00c2\7\20\2\2\u00b2\u00b4\7\21\2\2\u00b3\u00b2\3\2\2\2\u00b4"+
		"\u00b7\3\2\2\2\u00b5\u00b3\3\2\2\2\u00b5\u00b6\3\2\2\2\u00b6\u00b8\3\2"+
		"\2\2\u00b7\u00b5\3\2\2\2\u00b8\u00bc\7\r\2\2\u00b9\u00bb\7\21\2\2\u00ba"+
		"\u00b9\3\2\2\2\u00bb\u00be\3\2\2\2\u00bc\u00ba\3\2\2\2\u00bc\u00bd\3\2"+
		"\2\2\u00bd\u00bf\3\2\2\2\u00be\u00bc\3\2\2\2\u00bf\u00c1\7\20\2\2\u00c0"+
		"\u00b5\3\2\2\2\u00c1\u00c4\3\2\2\2\u00c2\u00c0\3\2\2\2\u00c2\u00c3\3\2"+
		"\2\2\u00c3\25\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c5\u00c6\7\5\2\2\u00c6\u00c7"+
		"\5\2\2\2\u00c7\27\3\2\2\2\u00c8\u00d3\5\f\7\2\u00c9\u00d3\7\20\2\2\u00ca"+
		"\u00d3\5\4\3\2\u00cb\u00d3\7\21\2\2\u00cc\u00d3\7\6\2\2\u00cd\u00d3\7"+
		"\16\2\2\u00ce\u00d3\7\17\2\2\u00cf\u00d3\5\6\4\2\u00d0\u00d3\5\b\5\2\u00d1"+
		"\u00d3\5\n\6\2\u00d2\u00c8\3\2\2\2\u00d2\u00c9\3\2\2\2\u00d2\u00ca\3\2"+
		"\2\2\u00d2\u00cb\3\2\2\2\u00d2\u00cc\3\2\2\2\u00d2\u00cd\3\2\2\2\u00d2"+
		"\u00ce\3\2\2\2\u00d2\u00cf\3\2\2\2\u00d2\u00d0\3\2\2\2\u00d2\u00d1\3\2"+
		"\2\2\u00d3\u00d6\3\2\2\2\u00d4\u00d2\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5"+
		"\31\3\2\2\2\u00d6\u00d4\3\2\2\2\34%\'EJOSX^djpw|\u0081\u0085\u008a\u008f"+
		"\u0093\u009e\u00a5\u00ac\u00b5\u00bc\u00c2\u00d2\u00d4";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}