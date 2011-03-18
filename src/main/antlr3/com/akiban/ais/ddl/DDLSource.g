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
 
grammar DDLSource;

tokens {

LEFT_PAREN = '(';
RIGHT_PAREN = ')';
LEFT_BRACKET = '\{';
RIGHT_BRACKET = '\}';
COMMA = ',';
SEMICOLON = ';';
DOT	=	 '.';
EQUALS = '=';
USE = 'use';
TABLE = 'table';
CREATE = 'create';
PRIMARY = 'primary';
INDEX = 'index' ;
KEY = 'key';
FOREIGN = 'foreign';
UNIQUE = 'unique';
UNSIGNED = 'unsigned';
ENGINE = 'engine';
VARYING = 'varying';
BINARY = 'binary';
VARBINARY = 'varbinary';
TINYINT = 'tinyint';
SMALLINT = 'smallint';
MEDIUMINT = 'mediumint';
INT = 'int';
INTEGER = 'integer';
BIGINT = 'bigint';
REAL = 'real';
DOUBLE = 'double';
FLOAT = 'float';
DEC = 'dec';
DECIMAL = 'decimal';
NUMERIC = 'numeric';
DATE = 'date';
DATETIME = 'datetime';
TIMESTAMP = 'timestamp';
TIME = 'time';
YEAR = 'year';
CHAR = 'char';
CHARACTER = 'character';
VARCHAR = 'varchar';
TINYBLOB = 'tinyblob';
BLOB = 'blob';
MEDIUMBLOB = 'mediumblob';
LONGBLOB = 'longblob';
TINYTEXT = 'tinytext';
TEXT = 'text';
MEDIUMTEXT = 'mediumtext';
LONGTEXT = 'longtext';
BIT = 'bit' ;
ENUM = 'enum';
SET = 'set';
NOT = 'not' ;
IF = 'if' ;
EXISTS = 'exists' ;
TEMPORARY = 'temporary' ;
NULL = 'null' ;
DEFAULT = 'default' ;
AUTO_INCREMENT = 'auto_increment';
CHARSET = 'charset';
COLLATE = 'collate';
REFERENCES = 'references';
ASC = 'asc';
DESC = 'desc';
ON = 'on';
DELETE = 'delete';
UPDATE = 'update';
COMMENT = 'comment';
CONSTRAINT = 'constraint';
RESTRICT = 'restrict';
CASCADE = 'cascade';
NO = 'no';
ACTION = 'action';
USING = 'using';
BTREE = 'btree';
HASH = 'hash';
KEY_BLOCK_SIZE = 'key_block_size';
WITH = 'with';
PARSER = 'parser';
FULLTEXT = 'fulltext';
SPATIAL = 'spatial';
FIXED = 'fixed';
SIGNED = 'signed';
SERIAL = 'serial';
VALUE = 'value';
LIKE = 'like';
BOOL = 'bool';
BOOLEAN = 'boolean';
NATIONAL = 'national';
NCHAR = 'nchar';
NVARCHAR = 'nvarchar';
GEOMETRY = 'geometry';
GEOMETRYCOLLECTION = 'geometrycollection';
POINT = 'point';
MULTIPOINT  = 'multipoint';
LINESTRING = 'linestring';
MULTILINESTRING = 'multilinestring';
POLYGON = 'polygon';
MULTIPOLYGON = 'multipolygon';
PRECISION = 'precision';
}

@header {
package com.akiban.ais.ddl;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDef.CName;
}

@lexer::header {
package com.akiban.ais.ddl;
}

schema[SchemaDef schema]
	: schema_ddl[$schema]+ EOF
    ;
    
cname[SchemaDef schema] returns[CName cname]
    : (schemaName=qname DOT)? tableName=qname  {return new CName($schema, $schemaName.name, $tableName.name);}
    ;
    
schema_ddl[SchemaDef schema]
    : (table[$schema] | use[$schema]) SEMICOLON?
    ;
 
use[SchemaDef schema]
    : USE qname {schema.use($qname.name);}
    ; 
    
table[SchemaDef schema]
	: CREATE TABLE (IF NOT EXISTS)? 
	  (table_spec[$schema] | 
	   dst=cname[$schema] LIKE src=cname[$schema] {$schema.addLikeTable($dst.cname, $src.cname);})
 	;

table_spec[SchemaDef schema]
	: table_name[$schema] 
	  LEFT_PAREN
	    table_element[$schema] (COMMA table_element[$schema])* 
	  RIGHT_PAREN
	  {$schema.finishTable();}
	  (table_suffix[$schema])* 
	  {$schema.resolveAllIndexes();}
	;

table_name[SchemaDef schema]
    : tableName=cname[$schema] {$schema.addTable(tableName);}
    ;
    
table_suffix[SchemaDef schema]
	: (ENGINE EQUALS engine=qname {$schema.setEngine(engine);})
	| (AUTO_INCREMENT EQUALS NUMBER {$schema.autoIncrementInitialValue($NUMBER.text);})
	| (DEFAULT? character_set[$schema])
	| (DEFAULT? collation[$schema])
	| (ID EQUALS qvalue)
	| (COMMENT EQUALS? qvalue)
	;
	
table_element[SchemaDef schema]
	: column_specification[$schema]
	| key_constraint[$schema]? primary_key_specification[$schema] index_option[$schema]* { $schema.finishConstraint(SchemaDef.IndexQualifier.UNIQUE); }
	| key_constraint[$schema]? foreign_key_specification[$schema] index_option[$schema]* { $schema.finishConstraint(SchemaDef.IndexQualifier.FOREIGN_KEY); }
	| key_constraint[$schema]? unique__key_specification[$schema] index_option[$schema]*{ $schema.finishConstraint(SchemaDef.IndexQualifier.UNIQUE); }
	| other_key_specification[$schema] index_option[$schema]*
	;
	
column_specification[SchemaDef schema]
	:  qname data_type_def {$schema.addColumn($qname.name, $data_type_def.type, $data_type_def.len1, $data_type_def.len2);}
	   (column_constraints[$schema])
	;

column_constraints[SchemaDef schema]
	: ({schema.startColumnOption(); } column_constraint[schema] {schema.endColumnOption(); }) * {schema.endColumnOption(); }
	;

column_constraint[SchemaDef schema]
	: NULL {$schema.nullable(true);}
	| NOT NULL {$schema.nullable(false);}
	| DEFAULT qvalue  {$schema.otherConstraint("DEFAULT=" + $qvalue.text);}
	| AUTO_INCREMENT {$schema.autoIncrement();}
	| ON UPDATE qvalue
	| COMMENT EQUALS? qvalue {$schema.addColumnComment($qvalue.text);}
	| character_set[$schema]
	| collation[$schema]
	| ID {$schema.otherConstraint($ID.text);}
	| KEY {$schema.seeKEY();}
	| PRIMARY {$schema.seePRIMARY();}
	| UNIQUE {$schema.seeUNIQUE();}
	| SERIAL DEFAULT VALUE {$schema.serialDefaultValue();}
	;

key_constraint[SchemaDef schema]
	: CONSTRAINT (qn=qname?) { $schema.setConstraintName($qn.name); }
	;

primary_key_specification[SchemaDef schema]
	: PRIMARY KEY index_type[$schema]? {$schema.startPrimaryKey();}
	  LEFT_PAREN primary_key_column[$schema] (COMMA primary_key_column[$schema])*
	  RIGHT_PAREN
	;
	
primary_key_column[SchemaDef schema]
	: qname {$schema.addPrimaryKeyColumn($qname.name); }
	;
	
other_key_specification[SchemaDef schema]
	: (FULLTEXT | SPATIAL)? (KEY | INDEX)? qname? {$schema.addIndex($qname.name);}
	index_type[$schema]?
	LEFT_PAREN index_key_column[$schema] (COMMA index_key_column[$schema])* RIGHT_PAREN
	;

foreign_key_specification[SchemaDef schema]
	: FOREIGN KEY qn1=qname? {$schema.addIndex($qn1.name, SchemaDef.IndexQualifier.FOREIGN_KEY);} 
		index_type[$schema]?
	    LEFT_PAREN index_key_column[$schema] (COMMA index_key_column[$schema])* RIGHT_PAREN
		REFERENCES refTable=cname[$schema] {$schema.addIndexReference(refTable);}
		LEFT_PAREN reference_column[$schema] (COMMA reference_column[$schema])* RIGHT_PAREN
		fk_cascade_clause*
	;

fk_cascade_clause
	: ON (UPDATE | DELETE) reference_option
	;

unique__key_specification[SchemaDef schema]
	: UNIQUE (KEY | INDEX)? qname? {$schema.addIndex($qname.name, SchemaDef.IndexQualifier.UNIQUE);}
		index_type[$schema]?
	  LEFT_PAREN index_key_column[$schema] (COMMA index_key_column[$schema])* RIGHT_PAREN
	;
	
index_key_column[SchemaDef schema]
	: qname {$schema.addIndexColumn($qname.name); }
	  (LEFT_PAREN NUMBER RIGHT_PAREN {$schema.setIndexedLength($NUMBER.text);} )?
	  (ASC | DESC {$schema.setIndexColumnDesc();})?
	;

index_type[SchemaDef schema]
	: USING (BTREE | HASH)
	;
	
index_option[SchemaDef schema]
	: KEY_BLOCK_SIZE EQUALS qvalue
	| index_type[$schema]
	| WITH PARSER qname
	| COMMENT qvalue
	;
	
reference_column[SchemaDef schema]
    : qname {$schema.addIndexReferenceColumn($qname.name); }
    ;
    
reference_option returns [String option]
	: RESTRICT {$option = "RESTRICT";}
	| CASCADE {$option = "CASCADE";}
	| SET NULL {$option = "SET NULL";}
	| NO ACTION {$option = "NO ACTION";}
	;
	
character_set[SchemaDef schema]
    : (CHARSET | CHARACTER SET) EQUALS? ID {$schema.addCharsetValue($ID.text);}
    ;
    
collation[SchemaDef schema]
	: COLLATE EQUALS? ID {$schema.addCollateValue($ID.text);}
	;
    
data_type_def returns [String type, String len1, String len2]
	: data_type {$type = $data_type.type;} (length_constraint {$len1 = $length_constraint.len1;})?
	| numeric_data_type {$type = $numeric_data_type.type;}  
	  (length_constraint {$len1 = $length_constraint.len1;})? 
	  (SIGNED | UNSIGNED {$type=$type + " UNSIGNED";})?
	| decimal_data_type {$type = $decimal_data_type.type;}
	  (decimal_constraint {$len1 = $decimal_constraint.len1; $len2 = $decimal_constraint.len2;})? 
	  (SIGNED | UNSIGNED {$type=$type + " UNSIGNED";})?
	| enum_or_set_data_type {$type = $enum_or_set_data_type.type; $len1 = $enum_or_set_data_type.len1;}
	| spatial_data_type {$type = $spatial_data_type.type;}
	;

data_type returns [String type]
	: DATE {$type = "DATE";}
	| DATETIME {$type = "DATETIME";}
	| TIMESTAMP {$type = "TIMESTAMP";}
	| TIME {$type = "TIME";}
	| YEAR {$type = "YEAR";}
	| (CHAR | CHARACTER) {$type = "CHAR";}
	| (NCHAR | NATIONAL (CHAR | CHARACTER)) {$type = "NCHAR";}
	| (VARCHAR | (CHAR | CHARACTER) VARYING) {$type = "VARCHAR";}
	| (NVARCHAR | NATIONAL ((CHAR | CHARACTER) VARYING | VARCHAR)) {$type = "NVARCHAR";}
	| TINYBLOB {$type = "TINYBLOB";}
	| BLOB {$type = "BLOB";}
	| MEDIUMBLOB {$type = "MEDIUMBLOB";}
	| LONGBLOB {$type = "LONGBLOB";}
	| TINYTEXT {$type = "TINYTEXT";}
	| TEXT {$type = "TEXT";}
	| MEDIUMTEXT {$type = "MEDIUMTEXT";}
	| LONGTEXT {$type = "LONGTEXT";}
	| BIT {$type = "BIT";}
	| BINARY {$type = "BINARY";}
	| VARBINARY {$type = "VARBINARY";}
	| SERIAL {$type = "SERIAL";}
	;
	
numeric_data_type returns [String type]
	: (TINYINT | BOOL | BOOLEAN) {$type = "TINYINT";}
	| SMALLINT {$type = "SMALLINT";}
	| MEDIUMINT {$type = "MEDIUMINT";}
	| (INT | INTEGER) {$type = "INT";}
	| BIGINT {$type = "BIGINT";}
	;

decimal_data_type returns [String type]
    : (DECIMAL | DEC | FIXED | NUMERIC) {$type = "DECIMAL";} 
	| (DOUBLE | DOUBLE PRECISION | REAL) {$type = "DOUBLE";}   // Technically should depend on MySQL's REAL_AS_FLOAT
	| FLOAT {$type = "FLOAT";}
    ;

enum_or_set_data_type returns [String type, String len1]
    : ENUM {$type = "ENUM";} (eset_constraint {$len1 = Integer.toString($eset_constraint.count);})
    | SET {$type = "SET";} (eset_constraint {$len1 = Integer.toString($eset_constraint.count);})
    ;

spatial_data_type returns [String type]
	: GEOMETRY {$type = "GEOMETRY";}
	| GEOMETRYCOLLECTION {$type = "GEOMETRYCOLLECTION";}
	| POINT {$type = "POINT";}
	| MULTIPOINT  {$type = "MULTIPOINT";}
	| LINESTRING {$type = "LINESTRING";}
	| MULTILINESTRING {$type = "MULTILINESTRING";}
	| POLYGON {$type = "POLYGON";}
	| MULTIPOLYGON {$type = "MULTIPOLYGON";}
	;

length_constraint returns [String len1]
	: LEFT_PAREN NUMBER {$len1 = $NUMBER.text;} RIGHT_PAREN;

decimal_constraint returns [String len1, String len2]
	: LEFT_PAREN n1=NUMBER {$len1 = $n1.text;} (COMMA n2=NUMBER {$len2 = $n2.text;})? RIGHT_PAREN;

eset_constraint returns [int count]
    :LEFT_PAREN (count_quoted_strings {$count = $count_quoted_strings.count;}) RIGHT_PAREN;
    
count_quoted_strings returns [int count]
    : TICKVALUE {$count = 1;} (COMMA TICKVALUE {$count++;})*;	
    	
qname returns [String name]
	:	(ID  {$name = $ID.text; } )
	|   (QNAME {$name = $QNAME.text.substring(1, $QNAME.text.length()-1); }  )
	| (qname_from_unquoted_token {$name = $qname_from_unquoted_token.name;})
	;

// Tokens that can be used as unquoted identifiers, emperically identified
qname_from_unquoted_token  returns [String name]
	: (ACTION | AUTO_INCREMENT | BIT | BOOL | BOOLEAN | BTREE | CHARSET | COMMENT | DATE | 
	   DATETIME | ENGINE | ENUM | FIXED | HASH | KEY_BLOCK_SIZE | NATIONAL | NCHAR | NO | 
	   NVARCHAR | PARSER SERIAL | SIGNED | TEMPORARY | TEXT | TIME | TIMESTAMP | VALUE | YEAR |
	   GEOMETRY | GEOMETRYCOLLECTION | POINT | MULTIPOINT  | LINESTRING | MULTILINESTRING | 
	   POLYGON | MULTIPOLYGON)
	   {$name = tokenNames[input.LA(-1)];}
	;

qvalue returns [String value]
    :   ID {$value = $ID.text;}
    |   NUMBER {$value = $NUMBER.text;}
    |   TICKVALUE {$value = $TICKVALUE.text;}
    |   NULL
    ;
  
/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/
TICKVALUE: '\'' (~ '\'')* '\'';
fragment DIGIT :   '0'..'9' ;
NUMBER 	:	('-')? (DIGIT | DOT)+;
QNAME : '`' (.)* '`' ;
ID : ('a'..'z' | '_') ('a'..'z' | DIGIT | '_' | '$')*;
               
WS : ('\t' | ' ' | '\r' | '\n' | '\u000C')+ {$channel=HIDDEN;};
SQL_LINE_COMMENT : '-- ' (~('\r'|'\n'))* '\r'? '\n' {$channel=HIDDEN;};
SQL_MULTILINE_COMMENT : '/*' (options {greedy=false;} : .)* '*/' {$channel=HIDDEN;};

