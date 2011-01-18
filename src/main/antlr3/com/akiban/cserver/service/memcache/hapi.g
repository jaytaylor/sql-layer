grammar hapi;

options {
	k=1;
}

tokens {
	DOT = '.';
	COMMA = ',';
	COLON = ':';
	
	PAREN_OPEN = '(';
	PAREN_CLOSE = ')';
		
	EQ = '=';
	NOT = '!';
	GTE = '>=';
	GT = '>';
	LT = '<';
	LTE = '<=';
	NE = '!=';
	EQ = '=';
}

get_request
	:
	STRING // schema
	COLON
	STRING // table
	COLON
	predicate_using ?
	predicate (COMMA predicate)*
	;

predicate_using
	: PAREN_OPEN STRING PAREN_CLOSE
	;

predicate 
	: STRING op STRING
	;

op
	: EQ
	| NE
	| GT
	| GTE
	| LT
	| LTE
	;

STRING
	: ( S_CHAR S_CHAR* )
   	| '\'' Q_CHAR* '\''
  	; 

fragment
HEX_DIGIT	: ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
S_CHAR 	: ('0'..'9'|'a'..'z'|'A'..'Z');

fragment
Q_CHAR
	: ESC_SEQ | ~('\\'|'\''|' ');

fragment
ESC_SEQ
	:   '\\' ('b'|'t'|'n'|'f'|'r'|'\''|'\\')
	|   UNICODE_ESC
	;

fragment
UNICODE_ESC
	:   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
	;
