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
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MMTable implements Serializable, ModelNames {
    public static Table create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        String tableType = (String) map.get(table_tableType);
        String schemaName = (String) map.get(table_schemaName);
        String tableName = (String) map.get(table_tableName);
        Integer tableId = (Integer) map.get(table_tableId);
        String groupName = (String) map.get(table_groupName);
        Table table = null;
        if (tableType.equals("USER")) {
            table = UserTable.create(ais, schemaName, tableName, tableId);
        } else if (tableType.equals("GROUP")) {
            table = GroupTable.create(ais, schemaName, tableName, tableId);
        }
        if (table != null && groupName != null) {
            Group group = ais.getGroup(groupName);
            table.setGroup(group);
        }
        assert table != null;
        table.setTreeName((String) map.get(table_treeName));
        table.setMigrationUsage(Table.MigrationUsage.values()[(Integer) map.get(table_migrationUsage)]);
        return table;
    }

    public static Map<String, Object> map(Table table)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(table_tableType, table.isGroupTable() ? "GROUP" : "USER");
        map.put(table_schemaName, table.getName().getSchemaName());
        map.put(table_tableName, table.getName().getTableName());
        map.put(table_tableId, table.getTableId());
        map.put(table_groupName, table.getGroup() == null ? null : table.getGroup().getName());
        map.put(table_migrationUsage, table.getMigrationUsage().ordinal());
        map.put(table_treeName, table.getTreeName());
        return map;
    }
}
