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

@header {
	package com.akiban.cserver.service.memcache;
}

@lexer::header {
	package com.akiban.cserver.service.memcache;
}

get_request returns[HapiGetRequest request]
	: { request = new HapiGetRequest() ; }
	schema=STRING {request.setSchema($schema.text); }
	COLON
	table=STRING {request.setTable($table.text); }
	COLON
	predicate_using[request] ?
	predicate[request] (COMMA predicate[request])*
	{ request.validate(); }
	;

predicate_using[HapiGetRequest request]
	: PAREN_OPEN STRING PAREN_CLOSE { request.setUsingTable($STRING.text); }
	;

predicate [HapiGetRequest request]
	: s=STRING o=op p=STRING { request.addPredicate(s.getText(), o, p.getText()); }
	;

op returns [HapiGetRequest.Predicate.Operator op]
	: EQ {$op = HapiGetRequest.Predicate.Operator.EQ; }
	| NE {$op = HapiGetRequest.Predicate.Operator.NE; }
	| GT {$op = HapiGetRequest.Predicate.Operator.GT; }
	| GTE {$op = HapiGetRequest.Predicate.Operator.GTE; }
	| LT {$op = HapiGetRequest.Predicate.Operator.LT; }
	| LTE {$op = HapiGetRequest.Predicate.Operator.LTE; }
	;

STRING
	: ( S_CHAR S_CHAR* )
   	| '\'' Q_CHARS '\'' {
   		try {setText( java.net.URLDecoder.decode($Q_CHARS.text, "UTF-8") ); }
   		catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
   	}
  	; 

fragment
S_CHAR 	: ('0'..'9'|'a'..'z'|'A'..'Z');

fragment
Q_CHARS : Q_CHAR*;

fragment
Q_CHAR	: S_CHAR | URL_CHAR | URL_ESC;

fragment
URL_CHAR: '.'|'-'|'_'|'+';

fragment
URL_ESC	: '%' HEX_DIGIT HEX_DIGIT;

fragment
HEX_DIGIT	: ('0'..'9'|'a'..'f'|'A'..'F') ;
