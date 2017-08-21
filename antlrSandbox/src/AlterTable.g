grammar AlterTable;

/* Example: ALTER TABLE t1 ADD CONSTRAINT name FOREIGN KEY ("column", column3) REFERENCES t2(column2, "Horriblename"); */
    
ALTER 	:	('A'|'a')('L'|'l')('T'|'t')('E'|'e')('R'|'r');
TABLE 	:	('T'|'t')('A'|'a')('B'|'b')('L'|'l')('E'|'e');
ADD 	:	('A'|'a')('D'|'d')('D'|'d');
CONSTRAINT :	('C'|'c')('O'|'o')('N'|'n')('S'|'s')('T'|'t')('R'|'r')('A'|'a')('I'|'i')('N'|'n')('T'|'t');
FOREIGN :	('F'|'f')('O'|'o')('R'|'r')('E'|'e')('I'|'i')('G'|'g')('N'|'n');
KEY 	:	('K'|'k')('E'|'e')('Y'|'y');
REFERENCES :	('R'|'r')('E'|'e')('F'|'f')('E'|'e')('R'|'r')('E'|'e')('N'|'n')('C'|'c')('E'|'e')('S'|'s');
LB	:	('(');
RB	:	(')');
SEMICOLON :	(';');
QUOTE	:	('"'|'`');
ID  	:	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|'$')*;
WS  	:   	(' ' | '\t' | '\r'| '\n') {$channel=HIDDEN;};


/* Grammar rules */

eval		:    	ALTER TABLE ID ADD (CONSTRAINT ID)? FOREIGN KEY LB ids RB REFERENCES ID LB ids RB SEMICOLON;
ids 		:	quotedId+;
quotedId	:	(QUOTE ID QUOTE) | ID;
