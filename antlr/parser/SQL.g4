grammar SQL;

FROM : ('f'|'F')('r'|'R')('o'|'O')('m'|'M');
JOIN : ('j'|'J')('o'|'O')('i'|'I')('n'|'N');
ON : ('o'|'O')('n'|'N');
AND : ('a'|'A')('n'|'N')('d'|'D');
USING : ('u'|'U')('s'|'S')('i'|'I')('n'|'N')('g'|'G');
DATEDIFF : ('d'|'D')('a'|'A')('t'|'T')('e'|'E')('d'|'D')('i'|'I')('f'|'F')('f'|'F');
DATETONUMBER : ('d'|'D')('a'|'A')('t'|'T')('e'|'E')('t'|'T')('o'|'O')('n'|'N')('u'|'U')('m'|'M')('b'|'B')('e'|'E')('r'|'R');
CORR: ('c'|'C')('o'|'O')('r'|'R')('r'|'R');
LBR : '(';
RBR : ')';
COMMA : ',';
EQUALS	: '=';
ARITHMETIC : ('*'|'/'|'+'|'-'|'^'); // Arithmetic operands define token borders, just like white space characters
TEXT : ~(' '|'\t'|'\r'|'\n'|'*'|'/'|'+'|'-'|'^'|'('|')'|','|'=')+; // Everything except WS, arithmetic, brackets, COMMA and EQUALS
WS : (' '|'\t'|'\r'|'\n');	// The space between the quotes is intentional

//SINGLE_LINE_COMMENT : '--' ~[\r\n]* -> channel(HIDDEN);
//MULTILINE_COMMENT : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN);

//start_rule: expression;	// Dummy start rule as ANTLR v3 requires, that the start rule is not referenced by another rule.

expression : (from | TEXT | bracket | WS | AND | EQUALS | ARITHMETIC | datediff | datetonumber | corr | COMMA)*;

bracket	: LBR expression RBR;

datediff : DATEDIFF LBR payload COMMA payload RBR;

datetonumber : DATETONUMBER LBR payload RBR;

corr : CORR LBR payload COMMA payload RBR;


// Using/On clause is not used in cross join -> it is optional
// Permit both, full outer join and outer join
from : FROM  WS* (bracket|table) WS* alias? (WS* TEXT* WS* TEXT* WS* JOIN WS* (bracket|table) WS* alias? WS* (using|on)?)*;

table : TEXT;

alias : TEXT;

using : USING WS* LBR WS* columns WS* RBR;

columns	: TEXT (WS*  COMMA WS* TEXT)*;

//on : ON WS* TEXT WS* EQUALS WS* TEXT (WS* AND WS* TEXT WS* EQUALS WS* TEXT)*;

on: ON expression;


payload	: (from | TEXT | bracket | WS | AND | EQUALS | ARITHMETIC | datediff | datetonumber | corr)*;




