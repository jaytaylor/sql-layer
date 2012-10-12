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

package com.akiban.sql.aisddl;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.akiban.ais.AISCloner;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.server.error.*;
import com.akiban.sql.parser.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.service.session.Session;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;

/** DDL operations on Indices */
public class IndexDDL
{
    private static final Logger logger = LoggerFactory.getLogger(IndexDDL.class);
    private IndexDDL() {
    }

    private static boolean returnHere(ExistenceCheck condition, InvalidOperationException error, QueryContext context)
    {
        switch(condition)
        {
            case IF_EXISTS:
                    // doesn't exist, does nothing
                context.warnClient(error);
                return true;
            case NO_CONDITION:
                throw error;
            default:
                throw new IllegalStateException("Unexpected condition in DROP INDEX: " + condition);
        }
    }

    public static void dropIndex (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    DropIndexNode dropIndex,
                                    QueryContext context) {
        String groupName = null;
        TableName tableName = null;
        ExistenceCheck condition = dropIndex.getExistenceCheck();

        final String indexSchemaName = dropIndex.getObjectName() != null && dropIndex.getObjectName().getSchemaName() != null ?
                dropIndex.getObjectName().getSchemaName() :
                    null;
        final String indexTableName = dropIndex.getObjectName() != null && dropIndex.getObjectName().getTableName() != null ?
                dropIndex.getObjectName().getTableName() :
                    null;
        final String indexName = dropIndex.getIndexName();
        List<String> indexesToDrop = Collections.singletonList(indexName); 
        
        // if the user supplies the table for the index, look only there
        if (indexTableName != null) {
            tableName = TableName.create(indexSchemaName == null ? defaultSchemaName : indexSchemaName, indexTableName);
            UserTable table = ddlFunctions.getAIS(session).getUserTable(tableName);
            if (table == null) {
                if(returnHere(condition, new NoSuchTableException(tableName), context))
                    return;
            }
            // if we can't find the index, set tableName to null
            // to flag not a user table index. 
            if (table.getIndex(indexName) == null) {
                tableName = null;
            }
            // Check the group associated to the table for the 
            // same index name. 
            Group group = table.getGroup();
            if (group.getIndex(indexName) != null) {
                // Table and it's group share an index name, we're confused. 
                if (tableName != null) {
                    throw new IndistinguishableIndexException(indexName);
                }
                // else flag group index for dropping
                groupName = group.getName();
            }
        } 
        // the user has not supplied a table name for the index to drop, 
        // scan all groups/tables for the index name
        else {
            for (UserTable table : ddlFunctions.getAIS(session).getUserTables().values()) {
                if (indexSchemaName != null && !table.getName().getSchemaName().equalsIgnoreCase(indexSchemaName)) {
                    continue;
                }
                if (table.getIndex(indexName) != null) {
                    if (tableName == null) {
                        tableName = table.getName();
                    } else {
                        throw new IndistinguishableIndexException(indexName);
                    }
                }
            }
            
            for (Group table : ddlFunctions.getAIS(session).getGroups().values()) {
                if (table.getIndex(indexName) != null) {
                    if (tableName == null && groupName == null) {
                        groupName = table.getName();
                    } else {
                        throw new IndistinguishableIndexException(indexName);
                    }
                }
            }
        }
        if (groupName != null) {
            ddlFunctions.dropGroupIndexes(session, groupName, indexesToDrop);
        } else if (tableName != null) {
            ddlFunctions.dropTableIndexes(session, tableName, indexesToDrop);
        } else {
            if(returnHere(condition, new NoSuchIndexException (indexName), context))
                return;
        }
    }

    public static void renameIndex (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName, 
                                    RenameNode renameIndex) {
        throw new UnsupportedSQLException (renameIndex.statementToString(), renameIndex); 
    }
    
    public static void createIndex(DDLFunctions ddlFunctions,
                                   Session session,
                                   String defaultSchemaName,
                                   CreateIndexNode createIndex)  {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);
        
        Collection<Index> indexesToAdd = new LinkedList<Index>();

        indexesToAdd.add(buildIndex(ais, defaultSchemaName, createIndex));
        
        ddlFunctions.createIndexes(session, indexesToAdd);
    }
    
    private static Index buildIndex (AkibanInformationSchema ais, String defaultSchemaName, CreateIndexNode index) {
        final String schemaName = index.getObjectName().getSchemaName() != null ? index.getObjectName().getSchemaName() : defaultSchemaName;
        final TableName tableName = TableName.create(schemaName, index.getIndexTableName().getTableName());
        
        if (checkIndexType (index, tableName) == Index.IndexType.TABLE) {
            logger.info ("Building Table index on table {}", tableName);
            return buildTableIndex (ais, tableName, index);
        } else {
            logger.info ("Building Group index on table {}", tableName);
            return buildGroupIndex (ais, tableName, index);
        }
    }

    /**
     * Check if the index specification is for a table index or a group index. We distinguish between 
     * them by checking the columns specified in the index, if they all belong to the table 
     * in the "ON" clause, this is a table index, else it is a group index. 
     * @param index AST for index to check
     * @param tableName ON clause table name
     * @return IndexType (GROUP or TABLE). 
     */
    private static Index.IndexType checkIndexType(CreateIndexNode index, TableName tableName) {
        for (IndexColumn col : index.getColumnList()) {
            
            // if the column has no table name (e.g. "col1") OR
            //    there is a table name (e.g. "schema1.table1.col1" or "table1.col1")
            //      if the table name has a schema it matches, else assume it's the same as the table
            //    and the table name match the index table. 
            
            if (col.getTableName() == null ||
                    ((col.getTableName().hasSchema() && 
                            col.getTableName().getSchemaName().equalsIgnoreCase(tableName.getSchemaName()) ||
                      !col.getTableName().hasSchema()) &&
                     col.getTableName().getTableName().equalsIgnoreCase(tableName.getTableName()))){
                ; // Column is in the base table, check the next
            } else {
                return Index.IndexType.GROUP;
            }
        }
        return Index.IndexType.TABLE;
    }
    
    private static Index buildTableIndex (AkibanInformationSchema ais, TableName tableName, CreateIndexNode index) {
        final String indexName = index.getObjectName().getTableName();

        UserTable table = ais.getUserTable(tableName);
        if (table == null) {
            throw new NoSuchTableException (tableName);
        }

        if (index.getJoinType() != null) {
            throw new TableIndexJoinTypeException();
        }

        AISBuilder builder = new AISBuilder();
        clone(builder, ais);
        
        builder.index(tableName.getSchemaName(), tableName.getTableName(), indexName, index.getUniqueness(),
                      index.getUniqueness() ? Index.UNIQUE_KEY_CONSTRAINT : Index.KEY_CONSTRAINT);

        int i = 0;
        IndexColumnList indexColumns = index.getColumnList();
        for (IndexColumn col : indexColumns) {
            Column tableCol = ais.getTable(tableName).getColumn(col.getColumnName());
            if (tableCol == null) {
                throw new NoSuchColumnException (col.getColumnName());
            }          
            builder.indexColumn(tableName.getSchemaName(), tableName.getTableName(), indexName, tableCol.getName(), i, col.isAscending(), null);
            i++;
        }
        builder.basicSchemaIsComplete();
        
        TableIndex tableIndex = builder.akibanInformationSchema().getTable(tableName).getIndex(indexName);

        if (indexColumns.functionType() == IndexColumnList.FunctionType.Z_ORDER_LAT_LON) {
            if (!Index.isSpatialCompatible(tableIndex)) {
                throw new BadSpatialIndexException(indexName, index);
            }
            tableIndex.markSpatial(indexColumns.firstFunctionArg(),
                                   indexColumns.lastFunctionArg() + 1 - indexColumns.firstFunctionArg());
            assert tableIndex.isSpatial() : tableIndex;
        }

        return tableIndex;
    }

    private static Index buildGroupIndex (AkibanInformationSchema ais, TableName tableName, CreateIndexNode index) {
        final String indexName = index.getObjectName().getTableName();
        
        if (ais.getUserTable(tableName) == null) {
            throw new NoSuchTableException (tableName);
        }
        
        final String groupName = ais.getUserTable(tableName).getGroup().getName();
        
        if (ais.getGroup(groupName) == null) {
            throw new NoSuchGroupException(groupName);
        }

        // TODO: Remove this check when the PSSM index creation does AIS.validate()
        // which is the correct approach. 
        if (index.getUniqueness()) {
            throw new UnsupportedUniqueGroupIndexException (indexName);
        }
        
        Index.JoinType joinType = null;
        if (index.getJoinType() == null) {
            throw new MissingGroupIndexJoinTypeException();
        }
        else {
            switch (index.getJoinType()) {
            case LEFT_OUTER:
                joinType = Index.JoinType.LEFT;
                break;
            case RIGHT_OUTER:
                joinType = Index.JoinType.RIGHT;
                break;
            case INNER:
                if (false) {        // TODO: Not yet supported; falls through to error.
//                joinType = Index.JoinType.INNER;
                break;
                }
            default:
                throw new UnsupportedGroupIndexJoinTypeException(index.getJoinType().toString());
            }
        }

        AISBuilder builder = new AISBuilder();
        clone(builder, ais);
        builder.groupIndex(groupName, indexName, index.getUniqueness(), joinType);
        
        int i = 0;
        String schemaName;
        TableName columnTable;
        for (IndexColumn col : index.getColumnList()) {
            if (col.getTableName() != null) {
                schemaName = col.getTableName().hasSchema() ? col.getTableName().getSchemaName() : tableName.getSchemaName();
                columnTable = TableName.create(schemaName, col.getTableName().getTableName());
            } else {
                columnTable = tableName;
                schemaName = tableName.getSchemaName();
            }

            final String columnName = col.getColumnName(); 

            if (ais.getUserTable(columnTable) == null) {
                throw new NoSuchTableException(columnTable);
            }
            
            if (ais.getUserTable(columnTable).getGroup().getName() != groupName)
                throw new IndexTableNotInGroupException(indexName, columnName, columnTable.getTableName());

            Column tableCol = ais.getUserTable(columnTable).getColumn(columnName); 
            if (tableCol == null) {
                throw new NoSuchColumnException (col.getColumnName());
            }
            
            builder.groupIndexColumn(groupName, indexName, schemaName, columnTable.getTableName(), columnName, i);
            i++;
        }
        builder.basicSchemaIsComplete();
        return builder.akibanInformationSchema().getGroup(groupName).getIndex(indexName);
    }

    private static void clone(AISBuilder builder, AkibanInformationSchema ais) {
        AISCloner.clone(builder.akibanInformationSchema(), ais, ProtobufWriter.ALL_SELECTOR);
    }
}
