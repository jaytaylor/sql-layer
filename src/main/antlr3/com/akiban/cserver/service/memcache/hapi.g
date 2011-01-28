grammar hapi;


options {
	k=1;
}

tokens {
	DOT = '.';
	COMMA = ',';
	COLON = ':';
	QUOTE = '\'';
	
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

@lexer::members {
	public static class HapiLexerException extends RuntimeException {
		private HapiLexerException(RecognitionException e) {
			super(e);
		}
	}
	
	@Override
	public void reportError(RecognitionException e) {
		throw new HapiLexerException(e);
	}
}
  
@members {
	private ParsedHapiGetRequest.ParseErrorReporter errorReporter;
	public void setErrorReporter(ParsedHapiGetRequest.ParseErrorReporter reporter) {
		if (reporter == null) {
			System.err.println("Given ParsedHapiGetRequest.ParseErrorReporter is null; will default to stderr");
		}
		this.errorReporter = reporter;
	}
	
	@Override
	public void emitErrorMessage(String message) {
		if (errorReporter == null) {
			System.err.println(message);
		}
		else {
			errorReporter.reportError(message);
		}
	}
}

get_request returns[ParsedHapiGetRequest request]
	: { request = new ParsedHapiGetRequest() ; }
	schema=string {request.setSchema(schema); }
	COLON
	table=string {request.setTable(table); }
	COLON
	predicate_using[request] ?
	predicate[request] (COMMA predicate[request])*
	{ request.validate(); }
	;

predicate_using[ParsedHapiGetRequest request]
	: PAREN_OPEN using=string PAREN_CLOSE { request.setUsingTable(using); }
	;

predicate [ParsedHapiGetRequest request]
	: s=string o=op p=string { request.addPredicate(s, o, p); }
	;

op returns [SimplePredicate.Operator op]
	: EQ {$op = SimplePredicate.Operator.EQ; }
	| NE {$op = SimplePredicate.Operator.NE; }
	| GT {$op = SimplePredicate.Operator.GT; }
	| GTE {$op = SimplePredicate.Operator.GTE; }
	| LT {$op = SimplePredicate.Operator.LT; }
	| LTE {$op = SimplePredicate.Operator.LTE; }
	;

string returns [String string]
	: S_CHARS { $string = ("NULL".equalsIgnoreCase($S_CHARS.text)) ? null : $S_CHARS.text; }
 	| QUOTE { final StringBuilder sb = new StringBuilder(); }
		(S_CHARS { sb.append($S_CHARS.text); } | Q_CHARS { sb.append($Q_CHARS.text); })*
		QUOTE {
 		try { $string = java.net.URLDecoder.decode(sb.toString(), "UTF-8"); }
		catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
 	}
	;

S_CHARS	: S_CHAR S_CHAR* ;

fragment
S_CHAR 	: ('0'..'9'|'a'..'z'|'A'..'Z'|'.'|'-'|'*'|'_');

Q_CHARS	: ('+' | URL_ESC)+;

fragment
URL_ESC	: '%' HEX_DIGIT HEX_DIGIT;

fragment
HEX_DIGIT	: ('0'..'9'|'a'..'f'|'A'..'F') ;
