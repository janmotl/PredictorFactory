grammar SQL;


DATEDIFF : ('d'|'D')('a'|'A')('t'|'T')('e'|'E')('d'|'D')('i'|'I')('f'|'F')('f'|'F');
DATETONUMBER : ('d'|'D')('a'|'A')('t'|'T')('e'|'E')('t'|'T')('o'|'O')('n'|'N')('u'|'U')('m'|'M')('b'|'B')('e'|'E')('r'|'R');
CORR: ('c'|'C')('o'|'O')('r'|'R')('r'|'R');
LBR : '(';
RBR : ')';
COMMA : ',';
TEXT : ~(' '|'\t'|'\r'|'\n'|'*'|'/'|'+'|'-'|'^'|'('|')'|',')+; // Everything except WS, arithmetic, brackets and COMMA
ARITHMETIC : ('*'|'/'|'+'|'-'|'^'); // Arithmetic operands define token borders, just like white space characters
WS : (' '|'\t'|'\r'|'\n');	// The space between the quotes is intentional

//SINGLE_LINE_COMMENT : '--' ~[\r\n]* -> channel(HIDDEN);
//MULTILINE_COMMENT : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN);

//start_rule: expression;	// Dummy start rule as ANTLR v3 requires, that the start rule is not referenced by another rule.

expression : (TEXT | bracket | WS | ARITHMETIC | datediff | datetonumber | corr | COMMA)*;

bracket	: LBR expression RBR;

datediff : DATEDIFF LBR payload COMMA payload RBR;

datetonumber : DATETONUMBER LBR payload RBR;

corr : CORR LBR payload COMMA payload RBR;

payload	: (TEXT | bracket | WS | ARITHMETIC | datediff | datetonumber | corr)*;




