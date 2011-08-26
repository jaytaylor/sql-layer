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

package com.akiban.sql.aisddl;

import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchGroupException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.error.UnsupportedUniqueGroupIndexException;
import com.akiban.server.service.session.Session;
import com.akiban.sql.parser.CreateIndexNode;
import com.akiban.sql.parser.DropIndexNode;
import com.akiban.sql.parser.IndexColumn;
import com.akiban.sql.parser.RenameNode;

import com.akiban.ais.io.AISTarget;
import com.akiban.ais.io.TableSubsetWriter;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;

/** DDL operations on Indices */
public class IndexDDL
{
    private static final Logger logger = LoggerFactory.getLogger(IndexDDL.class);
    private IndexDDL() {
    }

    public static void dropIndex (DDLFunctions ddlFunctions,
                                    Session session,
                                    String defaultSchemaName,
                                    DropIndexNode dropIndex) {
        
        throw new UnsupportedSQLException (dropIndex.statementToString(), dropIndex);
        //ddlFunctions.dropTableIndexes(session, tableName, indexesToDrop)
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
                            true) &&
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

        if (ais.getUserTable(tableName) == null) {
            throw new NoSuchTableException (tableName);
        }

        AISBuilder builder = new AISBuilder();
        addTable (builder, ais, tableName);
        
        builder.index(tableName.getSchemaName(), tableName.getTableName(), indexName, index.getUniqueness(),
                index.getUniqueness() ? Index.UNIQUE_KEY_CONSTRAINT : Index.KEY_CONSTRAINT);

        int i = 0;
        for (IndexColumn col : index.getColumnList()) {
            Column tableCol = ais.getTable(tableName).getColumn(col.getColumnName());
            if (tableCol == null) {
                throw new NoSuchColumnException (col.getColumnName());
            }          
            builder.indexColumn(tableName.getSchemaName(), tableName.getTableName(), indexName, tableCol.getName(), i, col.isAscending(), 0);
            i++;
        }
        builder.basicSchemaIsComplete();
        
        return builder.akibanInformationSchema().getTable(tableName).getIndex(indexName);
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

        if (index.getUniqueness()) {
            throw new UnsupportedUniqueGroupIndexException (indexName);
        }
        
        AISBuilder builder = new AISBuilder();
        addGroup(builder, ais, groupName);
        builder.groupIndex(groupName, indexName, index.getUniqueness());
        
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

    private static void addGroup (AISBuilder builder, AkibanInformationSchema ais, final String groupName) {

        new TableSubsetWriter(new AISTarget(builder.akibanInformationSchema())) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getGroup().getName().equalsIgnoreCase(groupName);
            }
        }.save(ais);
    }
    
    private static void addTable (AISBuilder builder, AkibanInformationSchema ais, final TableName tableName) {
        new TableSubsetWriter(new AISTarget(builder.akibanInformationSchema())) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getName().equals(tableName);
            }
        }.save(ais);
    }
}
