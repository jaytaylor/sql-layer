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
import com.akiban.ais.model.TableName;
import com.akiban.server.error.AISNullReferenceException;
import com.akiban.server.error.DuplicateColumnNameException;
import com.akiban.server.error.DuplicateGroupNameException;
import com.akiban.server.error.DuplicateIndexColumnException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.NameIsNullException;

public class AISInvariants {

    public static void checkNullField (Object field, String owner, String fieldName, String reference) {
        if (field == null) {
            throw new AISNullReferenceException(owner, fieldName, reference);
        }
    }
    
    public static void checkNullName (final String name, final String source, final String type) {
        if (name == null || name.length() == 0) {
            throw new NameIsNullException (source, type);
        }
    }
    
    public static void checkDuplicateTables(AkibanInformationSchema ais, String schemaName, String tableName)
    {
        if (ais.getTable(schemaName, tableName) != null) {
            throw new DuplicateTableNameException (new TableName(schemaName, tableName));
        }
    }
    
    public static void checkDuplicateColumnsInTable(Table table, String columnName)
    {
        if (table.getColumn(columnName) != null) {
            throw new DuplicateColumnNameException(table.getName(), columnName);
        }
    }
    public static void checkDuplicateColumnPositions(Table table, Integer position) {
        if (position < table.getColumnsIncludingInternal().size() && 
                table.getColumn(position) != null &&
                table.getColumn(position).getPosition() == position) {
            throw new DuplicateColumnNameException (table.getName(), table.getColumn(position).getName());
        }
    }
    
    public static void checkDuplicateColumnsInIndex(Index index, String columnName)
    {
        if (index.getKeyColumns().contains(columnName)) {
            throw new DuplicateIndexColumnException (index, columnName);
        }
    }
    
    public static void checkDuplicateIndexesInTable(Table table, String indexName) 
    {
        if (isIndexInTable(table, indexName)) {
            throw new DuplicateIndexException (table.getName(), indexName);
        }
    }
    
    public static boolean isIndexInTable (Table table, String indexName)
    {
        return table.getIndex(indexName) != null;
    }
 
    public static void checkDuplicateIndexColumnPosition (Index index, Integer position) {
        if (position < index.getKeyColumns().size()) {
            // TODO: Index uses position for a relative ordering, not an absolute position. 
        }
    }
    public static void checkDuplicateGroups (AkibanInformationSchema ais, String groupName)
    {
        if (ais.getGroup(groupName) != null) {
            throw new DuplicateGroupNameException (groupName);
        }
    }    
}
