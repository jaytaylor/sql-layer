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
