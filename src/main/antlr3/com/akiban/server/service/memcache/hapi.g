/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */


/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */
 
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

	LIMIT = ':LIMIT';
}

@header {
	package com.akiban.server.service.memcache;
}

@lexer::header {
	package com.akiban.server.service.memcache;
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
	predicate_options[request] ?
	predicate[request] (COMMA predicate[request])*
	{ request.validate(); }
	;

predicate_options[ParsedHapiGetRequest request]
	: PAREN_OPEN
	predicate_using[request] ?
	predicate_limit[request] ?
	PAREN_CLOSE
	;

predicate_using[ParsedHapiGetRequest request]
	: using=string { request.setUsingTable(using); }
	;

predicate_limit[ParsedHapiGetRequest request]
	: LIMIT EQ limit=string { request.setLimit(limit); }
	;

predicate [ParsedHapiGetRequest request]
	: s=string o=op p=string { request.addPredicate(s, o, p); }
	;

op returns [SimpleHapiPredicate.Operator op]
	: EQ {$op = SimpleHapiPredicate.Operator.EQ; }
	| NE {$op = SimpleHapiPredicate.Operator.NE; }
	| GT {$op = SimpleHapiPredicate.Operator.GT; }
	| GTE {$op = SimpleHapiPredicate.Operator.GTE; }
	| LT {$op = SimpleHapiPredicate.Operator.LT; }
	| LTE {$op = SimpleHapiPredicate.Operator.LTE; }
	;

string returns [String string]
	: S_CHARS { $string = ("NULL".equalsIgnoreCase($S_CHARS.text)) ? null : $S_CHARS.text; }
 	| QUOTE { final StringBuilder sb = new StringBuilder(); }
		(S_CHARS { sb.append($S_CHARS.text); } | Q_CHARS { sb.append($Q_CHARS.text); } )*
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
