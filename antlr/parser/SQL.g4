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

// Do not use ANTLR v3 anymore - it will complain and the complains are not silencable by using "options {backtrack=true;}".

// For Case-Insensitive Lexing:
// 		https://github.com/antlr/antlr4/blob/master/doc/case-insensitive-lexing.md

expression : (from | TEXT | bracket | WS | AND | EQUALS | ARITHMETIC | datediff | datediff_mssql | datetonumber | corr | COMMA)*;

bracket	: LBR expression RBR;

datediff : DATEDIFF LBR payload COMMA payload RBR;

datediff_mssql : DATEDIFF LBR payload COMMA payload COMMA payload RBR;	// MSSQL dialect is using 3 parameters and we do not want to modify this datediff()

datetonumber : DATETONUMBER LBR payload RBR;

corr : CORR LBR payload COMMA payload RBR;


// Using/On clause is not used in cross join -> it is optional
// Permit both, full outer join and outer join
// Permit implicit cross join join (tables separated by commas)
// We intentionally do not permit: "tableName as alias" as it is not supported in all databases
from : FROM  WS* (bracket|table) WS* alias? (WS* (TEXT* WS* TEXT* WS* JOIN|COMMA) WS* (bracket|table) WS* alias? WS* (using|on)?)*;

table : TEXT;

alias : TEXT;

using : USING WS* LBR WS* columns WS* RBR;

columns	: TEXT (WS*  COMMA WS* TEXT)*;

//on : ON WS* TEXT WS* EQUALS WS* TEXT (WS* AND WS* TEXT WS* EQUALS WS* TEXT)*;

on: ON expression;


payload	: (from | TEXT | bracket | WS | AND | EQUALS | ARITHMETIC | datediff | datediff_mssql | datetonumber | corr)*;




