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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    public void traversePreOrder(Visitor visitor) throws Exception
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
    public void traversePostOrder(Visitor visitor) throws Exception
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

    // Returns the columns in this table that are constrained to match the given column, e.g.
    // customer$cid and order$cid.
    public List<Column> matchingColumns(Column column)
    {
        assert column.getTable() == this;
        List<Column> matchingColumns = new ArrayList<Column>();
        matchingColumns.add(column);
        findMatchingAncestorColumns(column.getUserColumn(), matchingColumns);
        findMatchingDescendentColumns(column.getUserColumn(), matchingColumns);
        return matchingColumns;
    }

    private void findMatchingAncestorColumns(Column ancestorColumn, List<Column> matchingColumns)
    {
        Join join = ((UserTable)ancestorColumn.getTable()).getParentJoin();
        if (join != null) {
            JoinColumn ancestorJoinColumn = null;
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                if (joinColumn.getChild() == ancestorColumn) {
                    ancestorJoinColumn = joinColumn;
                }
            }
            if (ancestorJoinColumn != null) {
                Column groupColumn = ancestorJoinColumn.getParent().getGroupColumn();
                assert groupColumn.getTable() == this;
                matchingColumns.add(groupColumn);
                findMatchingAncestorColumns(ancestorJoinColumn.getParent(), matchingColumns);
            }
        }
    }

    private void findMatchingDescendentColumns(Column descendentColumn, List<Column> matchingColumns)
    {
        for (Join join : ((UserTable)descendentColumn.getTable()).getChildJoins()) {
            JoinColumn descendentJoinColumn = null;
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                if (joinColumn.getParent() == descendentColumn) {
                    descendentJoinColumn = joinColumn;
                }
            }
            if (descendentJoinColumn != null) {
                Column groupColumn = descendentJoinColumn.getChild().getGroupColumn();
                assert groupColumn.getTable() == this;
                matchingColumns.add(groupColumn);
                findMatchingDescendentColumns(descendentJoinColumn.getChild(), matchingColumns);
            }
        }
    }

    @SuppressWarnings("unused")
    private GroupTable()
    {
        super();
        // GWT requires empty constructor
    }
}
