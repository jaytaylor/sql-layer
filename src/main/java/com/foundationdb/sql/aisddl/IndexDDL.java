/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.aisddl;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.*;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.server.error.*;
import com.foundationdb.sql.parser.*;
import com.foundationdb.sql.parser.IndexColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.service.session.Session;

import com.foundationdb.qp.operator.QueryContext;

import static com.foundationdb.sql.aisddl.DDLHelper.skipOrThrow;

/** DDL operations on Indices */
public class IndexDDL
{
    private static final Logger logger = LoggerFactory.getLogger(IndexDDL.class);
    private IndexDDL() {
    }

    public static void dropIndex (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    DropIndexNode dropIndex,
                                    QueryContext context) {
        TableName groupName = null;
        TableName tableName = null;

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
            Table table = ddlFunctions.getAIS(session).getTable(tableName);
            if((table == null) &&
               skipOrThrow(context, dropIndex.getExistenceCheck(), table, new NoSuchTableException(tableName))) {
                return;
            }
            // if we can't find the index, set tableName to null
            // to flag not a user table index. 
            if (table.getIndex(indexName) == null && table.getFullTextIndex(indexName) == null) {
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
            for (Table table : ddlFunctions.getAIS(session).getTables().values()) {
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
            skipOrThrow(context, dropIndex.getExistenceCheck(), null, new NoSuchIndexException(indexName));
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
                                   CreateIndexNode createIndex,
                                   QueryContext context) {
        AkibanInformationSchema ais = ddlFunctions.getAIS(session);

        Index index = buildIndex(ddlFunctions, ais, defaultSchemaName, createIndex, context);
        if(index != null) {
            ddlFunctions.createIndexes(session, Collections.singleton(index));
        }
    }
    
    protected static Index buildIndex (DDLFunctions ddlFunctions,
                                       AkibanInformationSchema ais,
                                       String defaultSchemaName,
                                       CreateIndexNode createIndex,
                                       QueryContext context) {
        final String schemaName = createIndex.getObjectName().getSchemaName() != null ? createIndex.getObjectName().getSchemaName() : defaultSchemaName;
        final String indexName = createIndex.getObjectName().getTableName();
        final TableName tableName = TableName.create(schemaName, createIndex.getIndexTableName().getTableName());
        if (ais.getTable(tableName) == null) {
            throw new NoSuchTableException (tableName);
        }
        AISBuilder builder = new AISBuilder();
        clone(ddlFunctions.getAISCloner(), builder, ais);
        builder.getNameGenerator().mergeAIS(ais);
        Index index;
        TableName constraintName = null;
        if (createIndex.isUnique()) {
            constraintName = builder.getNameGenerator().generateUniqueConstraintName(schemaName, indexName);
        }
        if (createIndex.getIndexColumnList().functionType() == IndexColumnList.FunctionType.FULL_TEXT) {
            logger.debug ("Building Full text index on table {}", tableName) ;
            index = buildFullTextIndex (builder, tableName, indexName, createIndex, createIndex.getExistenceCheck(), context);
        } else if (checkIndexType (createIndex, tableName) == Index.IndexType.TABLE) {
            logger.debug ("Building Table index on table {}", tableName) ;
            index = buildTableIndex (builder, tableName, indexName, createIndex, constraintName, createIndex.getExistenceCheck(), context);
        } else {
            logger.debug ("Building Group index on table {}", tableName);
            index = buildGroupIndex (builder, tableName, indexName, createIndex, createIndex.getExistenceCheck(), context);
        }
        if(index != null) {
            boolean indexIsSpatial = createIndex.getIndexColumnList().functionType() == IndexColumnList.FunctionType.Z_ORDER_LAT_LON;
            // Can't check isSpatialCompatible before the index columns have been added.
            if(indexIsSpatial && !Index.isSpatialCompatible(index)) {
                throw new BadSpatialIndexException(index.getIndexName().getTableName(), createIndex);
            }
            builder.basicSchemaIsComplete();
            if(createIndex.getStorageFormat() != null) {
                TableDDL.setStorage(ddlFunctions, index, createIndex.getStorageFormat());
            }
        }
        return index;

    }

    /**
     * Check if the index specification is for a table index or a group index. We distinguish between 
     * them by checking the columns specified in the index, if they all belong to the table 
     * in the "ON" clause, this is a table index, else it is a group index. 
     * @param index AST for index to check
     * @param tableName ON clause table name
     * @return IndexType (GROUP or TABLE). 
     */
    protected static Index.IndexType checkIndexType(IndexDefinition index, TableName tableName) {
        for (IndexColumn col : index.getIndexColumnList()) {
            
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
    
    protected static Index buildTableIndex (AISBuilder builder,
                                            TableName tableName,
                                            String indexName,
                                            IndexDefinition index,
                                            TableName constraintName,
                                            ExistenceCheck existenceCheck,
                                            QueryContext context) {

        if (index.getJoinType() != null) {
            throw new TableIndexJoinTypeException();
        }

        Table curTable = builder.akibanInformationSchema().getTable(tableName);
        Index curIndex = curTable.getIndex(indexName);
        if(curIndex == null) {
            curIndex = curTable.getFullTextIndex(indexName);
        }

        if((curIndex != null) &&
           skipOrThrow(context, existenceCheck, curIndex, new DuplicateIndexException(tableName, indexName))) {
            return null;
        }

        builder.index(tableName.getSchemaName(), tableName.getTableName(), indexName, index.isUnique(),
                      false, constraintName);
        TableIndex tableIndex = builder.akibanInformationSchema().getTable(tableName).getIndex(indexName);
        IndexColumnList indexColumns = index.getIndexColumnList();
        if (indexColumns.functionType() == IndexColumnList.FunctionType.Z_ORDER_LAT_LON) {
            tableIndex.markSpatial(indexColumns.firstFunctionArg(),
                                   indexColumns.lastFunctionArg() + 1 - indexColumns.firstFunctionArg());
        }
        int i = 0;
        for (IndexColumn col : indexColumns) {
            Column tableCol = builder.akibanInformationSchema().getTable(tableName).getColumn(col.getColumnName());
            if (tableCol == null) {
                throw new NoSuchColumnException (col.getColumnName());
            }
            checkColAscending(col);
            builder.indexColumn(tableName.getSchemaName(),
                                tableName.getTableName(),
                                indexName,
                                tableCol.getName(),
                                i,
                                col.isAscending(),
                                null);
            i++;
        }
        return tableIndex;
    }

    protected static Index buildGroupIndex (AISBuilder builder,
                                            TableName tableName,
                                            String indexName,
                                            IndexDefinition index,
                                            ExistenceCheck existenceCheck,
                                            QueryContext context) {
        final TableName groupName = builder.akibanInformationSchema().getTable(tableName).getGroup().getName();

        Group curGroup = builder.akibanInformationSchema().getGroup(groupName);
        if (curGroup == null) {
            throw new NoSuchGroupException(groupName);
        }

        if (index.isUnique()) {
            throw new UnsupportedUniqueGroupIndexException (indexName);
        }

        Index curIndex = curGroup.getIndex(indexName);
        if((curIndex != null) &&
           skipOrThrow(context, existenceCheck, curIndex, new DuplicateIndexException(tableName, indexName))) {
            return null;
        }

        final Index.JoinType joinType;
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
                // Fall through as unsupported
            default:
                throw new UnsupportedGroupIndexJoinTypeException(index.getJoinType().toString());
            }
        }

        builder.groupIndex(groupName, indexName, index.isUnique(), joinType);
        GroupIndex groupIndex = builder.akibanInformationSchema().getGroup(groupName).getIndex(indexName);
        IndexColumnList indexColumns = index.getIndexColumnList();
        boolean indexIsSpatial = indexColumns.functionType() == IndexColumnList.FunctionType.Z_ORDER_LAT_LON;
        if (indexIsSpatial) {
            groupIndex.markSpatial(indexColumns.firstFunctionArg(),
                                   indexColumns.lastFunctionArg() + 1 - indexColumns.firstFunctionArg());
        }
        int i = 0;
        String schemaName;
        TableName columnTable;
        for (IndexColumn col : index.getIndexColumnList()) {
            if (col.getTableName() != null) {
                schemaName = col.getTableName().hasSchema() ? col.getTableName().getSchemaName() : tableName.getSchemaName();
                columnTable = TableName.create(schemaName, col.getTableName().getTableName());
            } else {
                columnTable = tableName;
                schemaName = tableName.getSchemaName();
            }

            final String columnName = col.getColumnName(); 

            if (builder.akibanInformationSchema().getTable(columnTable) == null) {
                throw new NoSuchTableException(columnTable);
            }
            
            if (builder.akibanInformationSchema().getTable(columnTable).getGroup().getName() != groupName)
                throw new IndexTableNotInGroupException(indexName, columnName, columnTable.getTableName());

            Column tableCol = builder.akibanInformationSchema().getTable(columnTable).getColumn(columnName); 
            if (tableCol == null) {
                throw new NoSuchColumnException (col.getColumnName());
            }

            checkColAscending(col);

            builder.groupIndexColumn(groupName, indexName, schemaName, columnTable.getTableName(), columnName, i);
            i++;
        }
        return builder.akibanInformationSchema().getGroup(groupName).getIndex(indexName);
    }

    protected static Index buildFullTextIndex (AISBuilder builder,
                                               TableName tableName,
                                               String indexName,
                                               IndexDefinition index,
                                               ExistenceCheck existenceCheck,
                                               QueryContext context) {
        Table table = builder.akibanInformationSchema().getTable(tableName);

        Index curIndex = table.getIndex(indexName);
        if(curIndex == null) {
            curIndex = table.getFullTextIndex(indexName);
        }
        if((curIndex != null) &&
           skipOrThrow(context, existenceCheck, curIndex, new DuplicateIndexException(tableName, indexName))) {
            return null;
        }

        if (index.getJoinType() != null) {
            throw new TableIndexJoinTypeException();
        }

        builder.fullTextIndex(tableName, indexName);
        int i = 0;
        String schemaName;
        TableName columnTable;
        for (IndexColumn col : index.getIndexColumnList()) {
            if (col.getTableName() != null) {
                schemaName = col.getTableName().hasSchema() ? col.getTableName().getSchemaName() : tableName.getSchemaName();
                columnTable = TableName.create(schemaName, col.getTableName().getTableName());
            } else {
                columnTable = tableName;
                schemaName = tableName.getSchemaName();
            }

            final String columnName = col.getColumnName(); 

            if (builder.akibanInformationSchema().getTable(columnTable) == null) {
                throw new NoSuchTableException(columnTable);
            }
            
            if (builder.akibanInformationSchema().getTable(columnTable).getGroup() != table.getGroup())
                throw new IndexTableNotInGroupException(indexName, columnName, columnTable.getTableName());

            Column tableCol = builder.akibanInformationSchema().getTable(columnTable).getColumn(columnName); 
            if (tableCol == null) {
                throw new NoSuchColumnException (col.getColumnName());
            }

            checkColAscending(col);
            
            builder.fullTextIndexColumn(tableName, indexName, schemaName, columnTable.getTableName(), columnName, i);
            i++;
        }
        return builder.akibanInformationSchema().getTable(tableName).getFullTextIndex(indexName);
    }

    private static void clone(AISCloner aisCloner, AISBuilder builder, AkibanInformationSchema ais) {
        aisCloner.clone(builder.akibanInformationSchema(), ais, ProtobufWriter.ALL_SELECTOR);
    }

    private static void checkColAscending(IndexColumn indexColumn) {
        if(!indexColumn.isAscending()) {
            throw new UnsupportedSQLException("DESC index column " + indexColumn.getColumnName());
        }
    }
}
