grammar datediff;



LBR : '(';
RBR : ')';
COMMA : ',';
DATEDIFF : ('d'|'D')('a'|'A')('t'|'T')('e'|'E')('d'|'D')('i'|'I')('f'|'F')('f'|'F');
TEXT : ~(' '|'\t'|'\r'|'\n'|'*'|'/'|'+'|'-'|'^'|'('|')'|','|'=')+; // Everything except WS, arithmetic, brackets, COMMA and EQUALS
WS : (' '|'\t'|'\r'|'\n');	// The space between the quotes is intentional



//start_rule  : expression;	// Dummy start rule as ANTLR v3 requires that the start rule is not referenced by another rule.

expression   : (TEXT | bracket | WS  | datediff | datediff_mssql | COMMA)*;

bracket	: LBR expression RBR;

datediff_mssql : DATEDIFF LBR payload COMMA payload COMMA payload RBR;

datediff : DATEDIFF LBR payload COMMA payload RBR;



payload	: ( TEXT | bracket | WS)*;




