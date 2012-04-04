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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;

import java.util.HashMap;
import java.util.Map;

public class MMTable implements ModelNames {
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
