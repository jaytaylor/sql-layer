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

import java.util.Collections;
import java.util.List;

public class HKeyColumn
{
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(column().getTable().getName().getTableName());
        buffer.append('.');
        buffer.append(column().getName());
        return buffer.toString();
    }

    public HKeySegment segment()
    {
        return segment;
    }

    public Column column()
    {
        return column;
    }

    public List<Column> equivalentColumns()
    {
        if (equivalentColumns == null) {
            assert column.getTable().isGroupTable() : "null equivalentColumns on non-group-table column: " + column;
            throw new UnsupportedOperationException("group tables have no equivalent columns");
        }
        return equivalentColumns;
    }

    public int positionInHKey()
    {
        return positionInHKey;
    }

    public HKeyColumn(HKeySegment segment, Column column)
    {
        this.segment = segment;
        this.column = column;
        this.positionInHKey = segment.positionInHKey() + segment.columns().size() + 1;
        if (column.getTable().isUserTable()) {
            UserTable userTable = (UserTable) column.getTable();
            this.equivalentColumns = Collections.unmodifiableList(userTable.matchingColumns(column));
        } else {
            this.equivalentColumns = null;
        }
    }

    // For use by this class
    
    private void findDependentTables(Column column, UserTable table, List<UserTable> dependentTables)
    {
        boolean dependent = false;
        for (HKeySegment segment : table.hKey().segments()) {
            for (HKeyColumn hKeyColumn : segment.columns()) {
                dependent = dependent || hKeyColumn.column() == column;
            }
        }
        if (dependent) {
            dependentTables.add(table);
        }
        for (Join join : table.getChildJoins()) {
            findDependentTables(column, join.getChild(), dependentTables);
        }
    }

    // State

    private final HKeySegment segment;
    private final Column column;
    private final int positionInHKey;
    // If column is a group table column, then we need to know all columns in the group table that are constrained
    // to have matching values, e.g. customer$cid and order$cid. For a user table, equivalentColumns contains just
    // column.
    private final List<Column> equivalentColumns;
}
