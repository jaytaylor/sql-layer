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
S_CHAR 	: ('0'..'9'|'a'..'z'|'A'..'Z');

fragment
Q_CHAR	: S_CHAR | URL_CHAR | URL_ESC;

fragment
URL_CHAR: '.'|'-'|'_';

fragment
URL_ESC	: '%' HEX_DIGIT HEX_DIGIT;

fragment
HEX_DIGIT	: ('0'..'9'|'a'..'f'|'A'..'F') ;
