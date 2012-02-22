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

package com.akiban.ais.model;

import java.util.Iterator;

public class GroupTable extends Table
{
    public static GroupTable create(AkibanInformationSchema ais,
                                        String schemaName,
                                        String tableName,
                                        Integer tableId)
    {
        GroupTable groupTable = new GroupTable(ais, schemaName, tableName, tableId);
        ais.addGroupTable(groupTable);
        return groupTable;
    }

    public GroupTable(AkibanInformationSchema ais, String schemaName, String tableName, Integer tableId)
    {
        super(ais, schemaName, tableName, tableId);
        engine = "AKIBANDB";
    }

    @Override
    public String toString()
    {
        return "GroupTable(" + super.toString() + " -> " + getRoot() + ")";
    }

    @Override
    public boolean isUserTable()
    {
        return false;
    }

    public UserTable getRoot()
    {
        UserTable tableInGroup = null;
        for (Iterator<UserTable> tables = ais.getUserTables().values().iterator();
             tableInGroup == null && tables.hasNext();) {
            UserTable table = tables.next();
            if (table.getGroup() == group) {
                tableInGroup = table;
            }
        }
        UserTable root = tableInGroup;
        while (root != null && root.getParentJoin() != null) {
            root = root.getParentJoin().getParent();
        }
        return root;
    }

    @Override
    public void setGroup(Group group)
    {
        if (group != null) {
            super.setGroup(group);
            group.setGroupTable(this);
        }
    }

    @Override
    public void traversePreOrder(Visitor visitor)
    {
        for (Column column : getColumns()) {
            visitor.visitColumn(column);
        }
        for (Index index : getIndexes()) {
            visitor.visitIndex(index);
            index.traversePreOrder(visitor);
        }
    }

    @Override
    public void traversePostOrder(Visitor visitor)
    {
        for (Column column : getColumns()) {
            visitor.visitColumn(column);
        }
        for (Index index : getIndexes()) {
            index.traversePostOrder(visitor);
            visitor.visitIndex(index);
        }
    }

    public void dropColumns()
    {
        for (Column groupColumn : getColumns()) {
            Column userColumn = groupColumn.getUserColumn();
            if(userColumn != null) {
                userColumn.setGroupColumn(null);
            }
        }
        super.dropColumns();
    }
}
