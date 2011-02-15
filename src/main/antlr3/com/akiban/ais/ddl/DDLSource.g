grammar DDLSource;

tokens {
// Symbols
COMMA = ',';
DOT	= '.';
EQUALS = '=';
LEFT_BRACKET = '\{';
LEFT_PAREN = '(';
RIGHT_BRACKET = '\}';
RIGHT_PAREN = ')';
SEMICOLON = ';';

// Numeric types
BIGINT = 'bigint';
BIT = 'bit' ;
DEC = 'dec';
DECIMAL = 'decimal';
DOUBLE = 'double';
FIXED = 'fixed';
FLOAT = 'float';
INT = 'int';
INTEGER = 'integer';
MEDIUMINT = 'mediumint';
NUMERIC = 'numeric';
REAL = 'real';
SERIAL = 'serial';
SMALLINT = 'smallint';
TINYINT = 'tinyint';

// String types
BINARY = 'binary';
BLOB = 'blob';
CHAR = 'char';
CHARACTER = 'character';
ENUM = 'enum';
LONGBLOB = 'longblob';
LONGTEXT = 'longtext';
MEDIUMBLOB = 'mediumblob';
MEDIUMTEXT = 'mediumtext';
SET = 'set';
TEXT = 'text';
TINYBLOB = 'tinyblob';
TINYTEXT = 'tinytext';
VARBINARY = 'varbinary';
VARCHAR = 'varchar';

// Date types
DATE = 'date';
DATETIME = 'datetime';
TIME = 'time';
TIMESTAMP = 'timestamp';
YEAR = 'year';

// Other types
FULLTEXT = 'fulltext';
SPATIAL = 'spatial';

// Other keywords
ACTION = 'action';
ASC = 'asc';
AUTO_INCREMENT = 'auto_increment';
BTREE = 'btree';
CASCADE = 'cascade';
CHARSET = 'charset';
COLLATE = 'collate';
CONSTRAINT = 'constraint';
CREATE = 'create';
DEFAULT = 'default';
DELETE = 'delete';
DESC = 'desc';
ENGINE = 'engine';
EXISTS = 'exists' ;
FOREIGN = 'foreign';
HASH = 'hash';
IF = 'if' ;
INDEX = 'index' ;
KEY_BLOCK_SIZE = 'key_block_size';
KEY = 'key';
MCOMMENT = 'comment';
NO = 'no';
NOT = 'not' ;
NULL = 'null' ;
ON = 'on';
PARSER = 'parser';
PRIMARY = 'primary';
REFERENCES = 'references';
RESTRICT = 'restrict';
SIGNED = 'signed';
TABLE = 'table';
TEMPORARY = 'temporary' ;
UNIQUE = 'unique';
UNSIGNED = 'unsigned';
UPDATE = 'update';
USE = 'use';
USING = 'using';
VARYING = 'varying';
WITH = 'with';
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
	: MULTILINE_COMMENT ? {$schema.comment($MULTILINE_COMMENT.text); }
    schema_ddl[$schema] + EOF
    ;
    
cname[SchemaDef schema] returns[CName cname]
    : (schemaName=qname DOT)? tableName=qname  {return new CName($schema, $schemaName.name, $tableName.name);}
    ;
    
schema_ddl[SchemaDef schema]
    : (table[$schema] | use[$schema]) SEMICOLON
    ;
 
use[SchemaDef schema]
    : USE qname {schema.use($qname.name);}
    ; 
    
table[SchemaDef schema]
	: CREATE TABLE (IF NOT EXISTS)? table_spec[$schema]
	;

table_spec[SchemaDef schema]
	: table_name[$schema] LEFT_PAREN
		table_element[$schema] (COMMA table_element[$schema])* RIGHT_PAREN
		{$schema.finishTable();} (table_suffix[$schema])* 
	{ $schema.resolveProvisionalIndexes(); }
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
	| (MCOMMENT EQUALS? qvalue)
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
	: AUTO_INCREMENT {$schema.autoIncrement();}
	| character_set[$schema]
	| collation[$schema]
	| DEFAULT qvalue {$schema.otherConstraint("DEFAULT=" + $qvalue.text);}
	| ID {$schema.otherConstraint($ID.text);}
	| KEY {$schema.seeKEY();}
	| NULL {$schema.nullable(true);}
	| NOT NULL {$schema.nullable(false);}
	| ON UPDATE qvalue
	| PRIMARY {$schema.seePRIMARY();}
	| MCOMMENT EQUALS? qvalue {$schema.addColumnComment($qvalue.text);}
	| UNIQUE {$schema.seeUNIQUE();}
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
	: FOREIGN KEY qn1=qname? {$schema.addIndex($qn1.name, SchemaDef.IndexQualifier.FOREIGN_KEY);  $schema.addIndexQualifier(SchemaDef.IndexQualifier.FOREIGN_KEY);} 
		index_type[$schema]?
	    LEFT_PAREN index_key_column[$schema] (COMMA index_key_column[$schema])* RIGHT_PAREN
		REFERENCES refTable=cname[$schema] {$schema.setIndexReference(refTable);}
		LEFT_PAREN reference_column[$schema] (COMMA reference_column[$schema])* RIGHT_PAREN
		fk_cascade_clause*
	;

fk_cascade_clause
	: ON (UPDATE | DELETE) reference_option
	;

unique__key_specification[SchemaDef schema]
	: UNIQUE (KEY | INDEX)? qname? {$schema.addIndex($qname.name);  $schema.addIndexQualifier(SchemaDef.IndexQualifier.UNIQUE);}
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
	| MCOMMENT qvalue
	;
	
reference_column[SchemaDef schema]
    : qname {$schema.addIndexReferenceColumn($qname.name); }
    ;
    
reference_option returns [String option]
	: CASCADE {$option = "CASCADE";}
	| NO ACTION {$option = "NO ACTION";}
	| RESTRICT {$option = "RESTRICT";}
	| SET NULL {$option = "SET NULL";}
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
	| SERIAL {$type = "SERIAL";}
	;

data_type returns [String type]
	: BIT {$type = "BIT";}
	| BINARY {$type = "BINARY";}
	| BLOB {$type = "BLOB";}
	| (CHAR | CHARACTER) {$type = "CHAR";}
	| DATE {$type = "DATE";}
	| DATETIME {$type = "DATETIME";}
	| MEDIUMBLOB {$type = "MEDIUMBLOB";}
	| LONGBLOB {$type = "LONGBLOB";}
	| LONGTEXT {$type = "LONGTEXT";}
	| MEDIUMTEXT {$type = "MEDIUMTEXT";}
	| TINYTEXT {$type = "TINYTEXT";}
	| TEXT {$type = "TEXT";}
	| TIMESTAMP {$type = "TIMESTAMP";}
	| TIME {$type = "TIME";}
	| TINYBLOB {$type = "TINYBLOB";}
	| VARBINARY {$type = "VARBINARY";}
	| (VARCHAR | CHARACTER VARYING) {$type = "VARCHAR";}
	| YEAR {$type = "YEAR";}
	;
	
numeric_data_type returns [String type]
	: BIGINT {$type = "BIGINT";}
	| (INT | INTEGER) {$type = "INT";}
	| MEDIUMINT {$type = "MEDIUMINT";}
	| SMALLINT {$type = "SMALLINT";}
	| TINYINT {$type = "TINYINT";}
	;

decimal_data_type returns [String type]
    : (DECIMAL | DEC | FIXED | NUMERIC) {$type = "DECIMAL";} 
	| (DOUBLE | REAL) {$type = "DOUBLE";}   // Technically should depend on MySQL's REAL_AS_FLOAT
	| FLOAT {$type = "FLOAT";}
    ;

enum_or_set_data_type returns [String type, String len1]
    : ENUM {$type = "ENUM";} (eset_constraint {$len1 = Integer.toString($eset_constraint.count);})
    | SET {$type = "SET";} (eset_constraint {$len1 = Integer.toString($eset_constraint.count);})
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
	: (ID {$name = $ID.text;})
	| (QNAME {$name = $QNAME.text.substring(1, $QNAME.text.length()-1);})
	| (qname_from_unquoted_token {$name = $qname_from_unquoted_token.name;})
	;

// Tokens that can be used as unquoted identifiers, emperically identified
qname_from_unquoted_token  returns [String name]
	: (ACTION | AUTO_INCREMENT | BIT | BTREE | CHARSET | COMMENT | DATE | 
	   DATETIME | ENGINE | ENUM | FIXED | HASH | KEY_BLOCK_SIZE | NO | 
	   PARSER | SERIAL | SIGNED | TEMPORARY | TEXT | TIME | TIMESTAMP | YEAR)
	   {$name = tokenNames[input.LA(-1)];}
	;

qvalue returns [String value]
    : ID {$value = $ID.text;}
    | NUMBER {$value = $NUMBER.text;}
    | TICKVALUE {$value = $TICKVALUE.text;}
    | NULL
    ;
  
/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/
WS : ( '\t' | ' ' | '\r' | '\n' | '\u000C' )+ 	{ $channel = HIDDEN; } ;
TICKVALUE: '\'' (~ '\'')* '\'';
fragment DIGIT :   '0'..'9' ;
NUMBER 	:	('-')? (DIGIT | DOT)+;
QNAME : '`' (.)* '`' ;
ID : ('a'..'z' | '_') ('a'..'z' | DIGIT | '_' | '$')*;
COMMENT: '--' (~('\r' | '\n'))* ('\r' | '\n')  { $channel = HIDDEN; } ;
IGNORE: ('/*' | '*/')+  { $channel = HIDDEN; } ;
MULTILINE_COMMENT :   '/*' (options {greedy=false;} : .)* '*/';

