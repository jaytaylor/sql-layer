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
FLOAT = 'float';
DOUBLE = 'double';
DECIMAL = 'decimal';
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
VARBIT = 'varbit' ;
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
MCOMMENT = 'comment';
CONSTRAINT = 'constraint';

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
	| (collation[$schema])
	| (ID EQUALS qvalue)
	;
	
table_element[SchemaDef schema]
	: column_specification[$schema]
	| key_constraint[$schema]? primary_key_specification[$schema] { $schema.finishConstraint(SchemaDef.IndexQualifier.UNIQUE); }
	| key_constraint[$schema]? foreign_key_specification[$schema] { $schema.finishConstraint(SchemaDef.IndexQualifier.FOREIGN_KEY); }
	| other_key_specification[$schema]
	;
	
column_specification[SchemaDef schema]
	:  qname data_type_def {$schema.addColumn($qname.name, $data_type_def.type, $data_type_def.len1, $data_type_def.len2);}
	   (column_constraint[$schema])*
	;

column_constraint[SchemaDef schema]
	: NULL {$schema.nullable(true);}
	| NOT NULL {$schema.nullable(false);}
	| DEFAULT qvalue  {$schema.otherConstraint("DEFAULT=" + $qvalue.text);}
	| AUTO_INCREMENT {$schema.autoIncrement();}
	| ON UPDATE qvalue
	| MCOMMENT EQUALS? TICKVALUE {$schema.addColumnComment($TICKVALUE.text);}
	| character_set[$schema]
	| collation[$schema]
	| ID {$schema.otherConstraint($ID.text);}
	| PRIMARY ? KEY {$schema.inlineColumnPK();}
	;

key_constraint[SchemaDef schema]
	: CONSTRAINT (qn=qname?) { $schema.setConstraintName($qn.name); }
	;

primary_key_specification[SchemaDef schema]
	: PRIMARY KEY {$schema.startPrimaryKey();}
	  LEFT_PAREN primary_key_column[$schema] (COMMA primary_key_column[$schema])*
	  RIGHT_PAREN
	;
	
primary_key_column[SchemaDef schema]
	: qname {$schema.addPrimaryKeyColumn($qname.name); }
	;
	
other_key_specification[SchemaDef schema]
	: ( unique__key_specification[$schema]
	  | nonunique_key_specification[$schema]
	  )
	LEFT_PAREN index_key_column[$schema] (COMMA index_key_column[$schema])* RIGHT_PAREN
	;

foreign_key_specification[SchemaDef schema]
	: FOREIGN KEY qn1=qname? {$schema.addIndex($qn1.name);  $schema.addIndexQualifier(SchemaDef.IndexQualifier.FOREIGN_KEY);} 
	    LEFT_PAREN index_key_column[$schema] (COMMA index_key_column[$schema])* RIGHT_PAREN
		REFERENCES refTable=cname[$schema] {$schema.setIndexReference(refTable);}
		LEFT_PAREN reference_column[$schema] (COMMA reference_column[$schema])* RIGHT_PAREN
		( ON DELETE qname)?
	;

unique__key_specification[SchemaDef schema]
	: UNIQUE (KEY | INDEX)? qname? {$schema.addIndex($qname.name);  $schema.addIndexQualifier(SchemaDef.IndexQualifier.UNIQUE);}
	;

nonunique_key_specification[SchemaDef schema]
	: (KEY | INDEX)? qname? {$schema.addIndex($qname.name);}
	;

index_key_column[SchemaDef schema]
	: qname {$schema.addIndexColumn($qname.name); }
	  (LEFT_PAREN NUMBER RIGHT_PAREN {$schema.setIndexedLength($NUMBER.text);} )?
	  (ASC | DESC {$schema.setIndexColumnDesc();})?
	;
	
reference_column[SchemaDef schema]
    : qname {$schema.addIndexReferenceColumn($qname.name); }
    ;
	
character_set[SchemaDef schema]
    : (CHARSET EQUALS? | CHARACTER SET) ID {$schema.addCharsetValue($ID.text);}
    ;
    
collation[SchemaDef schema]
	: COLLATE ID {$schema.addCollateValue($ID.text);}
	;
    
data_type_def returns [String type, String len1, String len2]
	: data_type {$type = $data_type.type;} (length_constraint {$len1 = $length_constraint.len1;})?
	| numeric_data_type {$type = $numeric_data_type.type;}  
	   (length_constraint {$len1 = $length_constraint.len1;})? 
	   (UNSIGNED {$type=$type + " UNSIGNED";})?
	| decimal_data_type 
	   {$type = $decimal_data_type.type; $len1 = $decimal_data_type.len1; 
	    $len2 = $decimal_data_type.len2;}
	| enum_or_set_data_type {$type = $enum_or_set_data_type.type; $len1 = $enum_or_set_data_type.len1;}
	;

data_type returns [String type]
	: CHARACTER VARYING {$type = "VARCHAR";}
	| BIT VARYING {$type = "VARBIT";}
	| DATE {$type = "DATE";}
	| DATETIME {$type = "DATETIME";}
	| TIMESTAMP {$type = "TIMESTAMP";}
	| TIME {$type = "TIME";}
	| YEAR {$type = "YEAR";}
	| CHAR {$type = "CHAR";}
	| CHARACTER {$type = "CHAR";}
	| VARCHAR {$type = "VARCHAR";}
	| TINYBLOB {$type = "TINYBLOB";}
	| BLOB {$type = "BLOB";}
	| MEDIUMBLOB {$type = "MEDIUMBLOB";}
	| LONGBLOB {$type = "LONGBLOB";}
	| TINYTEXT {$type = "TINYTEXT";}
	| TEXT {$type = "TEXT";}
	| MEDIUMTEXT {$type = "MEDIUMTEXT";}
	| LONGTEXT {$type = "LONGTEXT";}
	| BIT {$type = "BIT";}
	| VARBIT {$type = "VARBIT";}
	| BINARY {$type = "BINARY";}
	| VARBINARY {$type = "VARBINARY";}
	;
	
numeric_data_type returns [String type]
	: TINYINT {$type = "TINYINT";}
	| SMALLINT {$type = "SMALLINT";}
	| MEDIUMINT {$type = "MEDIUMINT";}
	| INT {$type = "INT";}
	| INTEGER {$type = "INT";}
	| BIGINT {$type = "BIGINT";}
	| FLOAT {$type = "FLOAT";}
	| DOUBLE {$type = "DOUBLE";}
	;

decimal_data_type returns [String type, String len1, String len2]
    : DECIMAL {$type = "DECIMAL";} 
      (decimal_constraint {$len1 = $decimal_constraint.len1; $len2 = $decimal_constraint.len2;})? 
      (UNSIGNED {$type=$type + " UNSIGNED";})?
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
	:	(ID  {$name = $ID.text; } )
	|   (QNAME {$name = $QNAME.text.substring(1, $QNAME.text.length()-1); }  )
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
WS : ( '\t' | ' ' | '\r' | '\n' | '\u000C' )+ 	{ $channel = HIDDEN; } ;
TICKVALUE: '\'' (~ '\'')* '\'';
fragment DIGIT :   '0'..'9' ;
NUMBER 	:	('-')? (DIGIT)+;
QNAME : '`' ('a'..'z' | '_') ('a'..'z' | DIGIT | '_' | '$' )* '`' ;
ID : ('a'..'z' | '_') ('a'..'z' | DIGIT | '_' | '$')*;
COMMENT: '--' (~('\r' | '\n'))* ('\r' | '\n')  { $channel = HIDDEN; } ;
IGNORE: ('/*' | '*/')+  { $channel = HIDDEN; } ;
MULTILINE_COMMENT :   '/*' (options {greedy=false;} : .)* '*/';

