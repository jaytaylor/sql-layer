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

package com.akiban.ais.metamodel;

public interface ModelNames
{
    String version = "version";
    // type
    String type = "type";
    String type_name = "name";
    String type_parameters = "parameters";
    String type_fixedSize = "fixedSize";
    String type_maxSizeBytes = "maxSizeBytes";
    String type_encoding = "encoding";
    // group
    String group = "group";
    String group_name = "groupName";
    // table
    String table = "table";
    String table_schemaName = "schemaName";
    String table_tableName = "tableName";
    String table_tableType = "tableType";
    String table_tableId = "tableId";
    String table_groupName = "groupName";
    String table_migrationUsage = "migrationUsage";
    String table_treeName = "treeName";
    // column
    String column = "column";
    String column_schemaName = "schemaName";
    String column_tableName = "tableName";
    String column_columnName = "columnName";
    String column_position = "position";
    String column_typename = "typename";
    String column_typeParam1 = "typeParam1";
    String column_typeParam2 = "typeParam2";
    String column_nullable = "nullable";
    String column_initialAutoIncrementValue = "initialAutoIncrementValue";
    String column_groupSchemaName = "groupSchemaName";
    String column_groupTableName = "groupTableName";
    String column_groupColumnName = "groupColumnName";
    String column_maxStorageSize = "maxStorageSize";
    String column_prefixSize = "prefixSize";
    String column_charset = "charset";
    String column_collation = "collation";
    // join
    String join = "join";
    String join_joinName = "joinName";
    String join_parentSchemaName = "parentSchemaName";
    String join_parentTableName = "parentTableName";
    String join_childSchemaName = "childSchemaName";
    String join_childTableName = "childTableName";
    String join_groupName = "groupName";
    String join_joinWeight = "joinWeight";
    String join_groupingUsage = "groupingUsage";
    String join_sourceTypes = "sourceTypes";
    // joinColumn
    String joinColumn = "joinColumn";
    String joinColumn_joinName = "joinName";
    String joinColumn_parentSchemaName = "parentSchemaName";
    String joinColumn_parentTableName = "parentTableName";
    String joinColumn_parentColumnName = "parentColumnName";
    String joinColumn_childSchemaName = "childSchemaName";
    String joinColumn_childTableName = "childTableName";
    String joinColumn_childColumnName = "childColumnName";
    // index
    String index = "index";
    String index_schemaName = "schemaName";
    String index_tableName = "tableName";
    String index_indexType = "indexType";
    String index_indexName = "indexName";
    String index_indexId = "indexId";
    String index_constraint = "constraint";
    String index_unique = "unique";
    String index_treeName = "treeName";
    // indexColumn
    String indexColumn = "indexColumn";
    String indexColumn_schemaName = "schemaName";
    String indexColumn_tableName = "tableName";
    String indexColumn_indexType = "indexType";
    String indexColumn_indexName = "indexName";
    String indexColumn_columnName = "columnName";
    String indexColumn_position = "position";
    String indexColumn_ascending = "ascending";
    String indexColumn_indexedLength= "indexedLength";
}
