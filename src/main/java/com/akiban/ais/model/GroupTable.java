/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * Created on: Nov, 20, 2009
 * Created by: Thomas Hazel
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GroupTable extends Table
{
    public static GroupTable create(AkibaInformationSchema ais,
                                        String schemaName,
                                        String tableName,
                                        Integer tableId)
    {
        GroupTable groupTable = new GroupTable(ais, schemaName, tableName, tableId);
        ais.addGroupTable(groupTable);
        return groupTable;
    }

    public GroupTable(AkibaInformationSchema ais, String schemaName, String tableName, Integer tableId)
    {
        super(ais, schemaName, tableName, tableId);
        engine = "AKIBANDB";
    }

    @Override
    public String toString()
    {
        return "GroupTable(" + super.toString() + " -> " + getRoot() + ", group(" + getGroup().getName() + "))";
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
            userColumn.setGroupColumn(null);
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
                ancestorColumn = ancestorJoinColumn.getParent();
                Column groupColumn = ancestorColumn.getGroupColumn();
                assert groupColumn.getTable() == this;
                matchingColumns.add(groupColumn);
                findMatchingAncestorColumns(ancestorColumn, matchingColumns);
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
                descendentColumn = descendentJoinColumn.getChild();
                Column groupColumn = descendentColumn.getGroupColumn();
                assert groupColumn.getTable() == this;
                matchingColumns.add(groupColumn);
                findMatchingDescendentColumns(descendentColumn, matchingColumns);
            }
        }
    }

    private GroupTable()
    {
        super();
        // GWT requires empty constructor
    }
}
