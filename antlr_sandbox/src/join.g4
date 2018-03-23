grammar Join;


FROM : ('f'|'F')('r'|'R')('o'|'O')('m'|'M');	
JOIN : ('j'|'J')('o'|'O')('i'|'I')('n'|'N');
ON : ('o'|'O')('n'|'N');	
AND : ('a'|'A')('n'|'N')('d'|'D');
USING : ('u'|'U')('s'|'S')('i'|'I')('n'|'N')('g'|'G');
LBR : '(';
RBR : ')';
COMMA : ',';
EQUALS	: '=';	
WORD : ~(' '|'\t'|'\r'|'\n'|'*'|'/'|'+'|'-'|'^'|'('|')'|','|'=')+; // Everything except WS, arithmetic, brackets, COMMA and EQUALS
WS : (' '|'\t'|'\r'|'\n')  { $channel = HIDDEN; };	// The space between the quotes is intentional
ARITHMETIC : ('*'|'/'|'+'|'-'|'^'); // Arythmetic operands define token borders, just like white space characters


//SINGLE_LINE_COMMENT : '--' ~[\r\n]* -> channel(HIDDEN);
//MULTILINE_COMMENT : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN);

start 	: query;

query : (WORD | COMMA | ARITHMETIC | bracket | from)* ;	

from : FROM (bracket|table) alias? (WORD* JOIN (bracket|table) alias? (using|on))*;

table : WORD;	

alias : WORD;	

bracket	: LBR query RBR;

using : USING LBR columns RBR;

on : ON WORD EQUALS WORD (AND WORD EQUALS WORD)*;	

columns	: WORD (COMMA WORD)*;










