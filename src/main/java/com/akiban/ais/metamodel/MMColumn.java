/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.ais.metamodel;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MMColumn implements Serializable, ModelNames {
    public static Column create(AkibanInformationSchema ais, Map<String, Object> map)
    {

        Column column = null;
        String schemaName = (String) map.get(column_schemaName);
        String tableName = (String) map.get(column_tableName);
        Table table = ais.getTable(schemaName, tableName);
        if (table != null) {
            Type type = ais.getType((String) map.get(column_typename));
            CharsetAndCollation charAndCol = CharsetAndCollation.intern((String) map.get(column_charset),
                                                                        (String) map.get(column_collation));
            Long param1 = null;
            Long param2 = null;
            Integer nParameters = type.nTypeParameters();
            if (nParameters >= 1) {
                param1 = (Long) map.get(column_typeParam1);
                if (nParameters >= 2) {
                    param2 = (Long) map.get(column_typeParam2);
                }
            }
            
            return Column.create(table,
                                 (String) map.get(column_columnName),
                                 (Integer) map.get(column_position),
                                 type,
                                 (Boolean) map.get(column_nullable),
                                 param1,
                                 param2,
                                 (Long) map.get(column_initialAutoIncrementValue),
                                 charAndCol);
        }
        return column;
    }


    public static Map<String, Object> map(Column column)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        String groupSchemaName = null;
        String groupTableName = null;
        String groupColumnName = null;
        Column groupColumn = column.getGroupColumn();
        if (groupColumn != null) {
            groupSchemaName = groupColumn.getTable().getName().getSchemaName();
            groupTableName = groupColumn.getTable().getName().getTableName();
            groupColumnName = groupColumn.getName();
        }
        Table table = column.getTable();
        map.put(column_schemaName, table.getName().getSchemaName());
        map.put(column_tableName, table.getName().getTableName());
        map.put(column_columnName, column.getName());
        map.put(column_position, column.getPosition());
        Type type = column.getType();
        map.put(column_typename, type.name());
        map.put(column_typeParam1, type.nTypeParameters() >= 1 ? column.getTypeParameter1() : null);
        map.put(column_typeParam2, type.nTypeParameters() >= 2 ? column.getTypeParameter2() : null);
        map.put(column_nullable, column.getNullable());
        map.put(column_maxStorageSize, column.getMaxStorageSize());
        map.put(column_prefixSize, column.getPrefixSize());
        map.put(column_initialAutoIncrementValue, column.getInitialAutoIncrementValue());
        map.put(column_groupSchemaName, groupSchemaName);
        map.put(column_groupTableName, groupTableName);
        map.put(column_groupColumnName, groupColumnName);
        CharsetAndCollation charsetAndCollation = column.getCharsetAndCollation();
        map.put(column_charset, charsetAndCollation.charset());
        map.put(column_collation, charsetAndCollation.collation());
        return map;
    }
}
