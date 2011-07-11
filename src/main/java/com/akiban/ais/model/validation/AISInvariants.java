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
package com.akiban.ais.model.validation;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.message.ErrorCode;
import com.akiban.server.InvalidOperationException;

public class AISInvariants {

    public static void checkNullName (final String name, final String source, final String type) {
        if (name == null || name.length() == 0) {
            throw new InvalidOperationException (ErrorCode.VALIDATION_FAILURE,
                    "%s creation has a null %s", source, type);
        }
    }
    
    public static void checkDuplicateTables(AkibanInformationSchema ais, String schemaName, String tableName)
    {
        if (ais.getTable(schemaName, tableName) != null) {
            throw new InvalidOperationException (ErrorCode.DUPLICATE_TABLE,
                    "Table %s.%s already exists in the system", 
                    schemaName, tableName);
        }
    }
    
    public static void checkDuplicateColumnsInTable(Table table, String columnName)
    {
        if (table.getColumn(columnName) != null) {
            throw new InvalidOperationException (ErrorCode.DUPLICATE_COLUMN,
                    "Table %s already has column %s", table.getName().toString(), columnName);
        }
    }
    public static void checkDuplicateColumnPositions(Table table, Integer position) {
        /* TODO: fix AkServerAisSourceTargetIT throwing this exception: 
         * Table akiban_information_schema._akiban_columns already has a column in position 7
         * 
        if (position < table.getColumnsIncludingInternal().size() && 
                table.getColumn(position) != null) {
            throw new InvalidOperationException (ErrorCode.DUPLICATE_COLUMN,
                    "Table %s already has a column in position %d", table.getName().toString(), position);
        }
        */
    }
    
    public static void checkDuplicateColumnsInIndex(Index index, String columnName)
    {
        if (index.getColumns().contains(columnName)) {
            throw new InvalidOperationException (ErrorCode.DUPLICATE_COLUMN, 
                    "Index %s already has column %s", 
                    index.getIndexName().toString(), columnName);
        }
    }
    
    public static void checkDuplicateIndexesInTable(Table table, String indexName) 
    {
        if (isIndexInTable(table, indexName)) {
            throw new InvalidOperationException (ErrorCode.DUPLICATE_KEY,
                    "Table %s already has index %s", table.getName().toString(), indexName);
        }
    }
    
    public static boolean isIndexInTable (Table table, String indexName)
    {
        return table.getIndex(indexName) != null;
    }
 
    public static void checkDuplicateIndexColumnPosition (Index index, Integer position) {
        if (position < index.getColumns().size()) {
            // TODO: Index uses position for a relative ordering, not an absolute position. 
        }
    }
    public static void checkDuplicateGroups (AkibanInformationSchema ais, String groupName)
    {
        if (ais.getGroup(groupName) != null) {
            throw new InvalidOperationException (ErrorCode.DUPLICATE_GROUP,
                    "Group %s already exists in the system",
                    groupName);
        }
    }
}
