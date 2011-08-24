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

import com.akiban.ais.model.TableIndex;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.WrongTableForIndexException;
import com.akiban.sql.parser.CreateIndexNode;
import com.akiban.sql.parser.TableName;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Index;

/** DDL operations on Indices */
public class IndexDDL
{
    private IndexDDL() {
    }

    public static void createIndex(AkibanInformationSchema ais,
                                   String defaultSchemaName,
                                   CreateIndexNode createIndex)  {
        TableName indexName = createIndex.getObjectName();
        TableName tableName = createIndex.getIndexTableName();
        String schemaName = tableName.getSchemaName();
        if (schemaName == null)
            schemaName = defaultSchemaName;
        UserTable table = ais.getUserTable(schemaName, tableName.getTableName());
        if (table == null)
            throw new NoSuchTableException (tableName.getSchemaName(), tableName.getTableName());
        // TODO: What about indexName schemaName?
        Index index = new TableIndex(table,
                                // TODO: Any case issues?
                                indexName.getTableName(),
                                -1, 
                                createIndex.getUniqueness(), 
                                null);

        // TODO: Not at all clear that the following is right.

        int pos = 0;
        for (com.akiban.sql.parser.IndexColumn cindexColumn : 
                 createIndex.getColumnList()) {
            com.akiban.ais.model.IndexColumn aindexColumn = 
                new com.akiban.ais.model.IndexColumn(index,
                                                     getColumn(ais, 
                                                               defaultSchemaName,
                                                               table,
                                                               cindexColumn.getTableName(),
                                                               cindexColumn.getColumnName()),
                                                     pos++,
                                                     cindexColumn.isAscending(),
                                                     null);
            index.getColumns().add(aindexColumn);
        }

        // TODO: Now what? table.addIndex(index)? Call something on
        // ais? Or did I need a session, which would make this
        // non-static?
    }

    protected static Column getColumn(AkibanInformationSchema ais, 
                                      String defaultSchemaName,
                                      UserTable defaultTable, 
                                      TableName tableName, String columnName) {
        UserTable table = defaultTable;
        if (tableName != null) {
            String schemaName = tableName.getSchemaName();
            if (schemaName == null)
                schemaName = defaultSchemaName;
            table = ais.getUserTable(schemaName, tableName.getTableName());
            if (table == null)
                throw new NoSuchTableException (tableName.getSchemaName(), tableName.getTableName());
        }
        if (!isAccessible(table, defaultTable))
            throw new WrongTableForIndexException (defaultTable.getName());
        Column column = table.getColumn(columnName);
        if (column == null)
            throw new NoSuchColumnException (columnName);
        return column;
    }

    protected static boolean isAccessible(UserTable table, UserTable fromTable) {
        while (true) {
            if (table == fromTable)
                return true;
            fromTable = fromTable.parentTable();
            if (fromTable == null)
                return false;
        }
    }

}
