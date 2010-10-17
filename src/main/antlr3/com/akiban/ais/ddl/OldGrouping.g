grammar OldGrouping;

tokens {

LEFT_PAREN = '(';
RIGHT_PAREN = ')';
LEFT_BRACKET = '\{';
RIGHT_BRACKET = '\}';

START_COMMENT = '/*' ;
END_COMMENT = '*/' ;

COMMA = ',';
DOT	=	 '.';
EQUALS = '=';
SEMICOLON = ';';
GROUP = 'group';
TABLE = 'table';
SCHEMA = 'schema';
GROUPSCHEMA = 'groupschema';
BASEID = 'baseid';

}

@header {
package com.akiban.ais.ddl;

import com.akiban.ais.ddl.GroupDef;

import com.akiban.ais.model.staticgrouping.GroupsBuilder;
import com.akiban.ais.model.staticgrouping.JoinDescriptionBuilder;
}

@lexer::header {
package com.akiban.ais.ddl;
}

old_grouping[GroupDef grouper]
	: START_COMMENT grouping[$grouper] END_COMMENT
	;

grouping[GroupDef grouper]
	: grouping_header[$grouper]  groups[$grouper.getGroupsBuilder()]
	;

/* HEADER */

grouping_header[GroupDef grouper]
	:SCHEMA name {$grouper.seeHeaderSchema($name.text);}
	(schema_base_id[$grouper])?
	(group_schema[$grouper])? SEMICOLON
	;
	    
group_schema[GroupDef grouper]
	: GROUPSCHEMA name {$grouper.seeHeaderGroupSchema($name.name);}
    ;
	
schema_base_id[GroupDef grouper]
	: BASEID EQUALS NUMBER  { $grouper.seeHeaderBaseid($NUMBER.text);}
    ;
    
/* GROUPS */
    
groups[GroupsBuilder grouper]
	: group[$grouper] (group[$grouper])*
	;

    
group[GroupsBuilder builder]
	: GROUP groupName=name
	LEFT_BRACKET groot[$builder, groupName.name] RIGHT_BRACKET SEMICOLON
	;
	
groot[GroupsBuilder builder, String groupName]
	: TABLE tableName=name { $builder.rootTable(tableName.name, groupName); $builder.startChildren(tableName.name); }
	(gtable_list[$builder])?
	{ $builder.endChildren(); }
	;

gtable_list[GroupsBuilder builder]
	: LEFT_BRACKET gtable[$builder]
	(COMMA  gtable[$builder])*
	RIGHT_BRACKET
	;
	
gtable[GroupsBuilder builder]
	: TABLE tableName=name { $builder.joinTables(tableName.name); $builder.startChildren(tableName.name); }
	(gtable_join_columns[builder.getLastJoinTablesBuilder()])
	(gtable_list[$builder])?
	{ $builder.endChildren(); }
	;

gtable_join_columns[JoinDescriptionBuilder joinBuilder]
	: LEFT_PAREN gtable_join_column[$joinBuilder] (COMMA gtable_join_column[$joinBuilder] )* RIGHT_PAREN
	;
	
gtable_join_column[JoinDescriptionBuilder joinBuilder] 
    : name {$joinBuilder.column($name.name, $name.name);} 
    ;
    
name returns [String name]
	:	ID  {$name = $ID.text; }
	;
    
/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/
 
WS : ( '\t' | ' ' | '\r' | '\n' | '\u000C' )+ 	{ $channel = HIDDEN; } ;
fragment DIGIT :   '0'..'9' ;
NUMBER 	:	('-')? (DIGIT)+;
ID : ('a'..'z' | '_') ('a'..'z' | DIGIT | '_' | '$')*;
