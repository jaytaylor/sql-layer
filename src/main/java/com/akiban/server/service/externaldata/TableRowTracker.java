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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;

import java.util.HashSet;
import java.util.Set;

public class TableRowTracker implements RowTracker {
    private final int minDepth;
    private final int maxDepth;
    // This is not sufficient if orphans are possible (when
    // ancestor keys are repeated in descendants). In that case, we
    // have to save rows and check that they are ancestors of new
    // rows, discarding any that are not.
    private final RowType[] openTypes;

    // Tracks child tables where the schema name does not match the parent.
    // Will almost always be empty, so use null as empty.
    private Set<UserTable> tablesNeedingFullName = null;

    private RowType curRowType;
    private UserTable curTable;
    private boolean curNeedsFullName;

    public TableRowTracker(UserTable table, int addlDepth) {
        minDepth = table.getDepth();
        final int max[] = { minDepth };
        if (addlDepth < 0) {
            table.traverseTableAndDescendants(new NopVisitor() {
                @Override
                public void visitUserTable(UserTable userTable) {
                    max[0] = Math.max(max[0], userTable.getDepth());
                    if(!userTable.isSchemaNameSameAsParent()) {
                        if(tablesNeedingFullName == null) {
                            tablesNeedingFullName = new HashSet<>();
                        }
                        tablesNeedingFullName.add(userTable);
                    }
                }
            });
        }
        else {
            max[0] += addlDepth;
        }
        maxDepth = max[0];
        openTypes = new RowType[maxDepth+1];
    }

    @Override
    public void reset() {
        curRowType = null;
        curTable = null;
    }

    @Override
    public int getMinDepth() {
        return minDepth;
    }

    @Override
    public int getMaxDepth() {
        return maxDepth;
    }

    @Override
    public void beginRow(Row row) {
        assert row.rowType().hasUserTable() : "Invalid row type for TableRowTracker";
        curRowType = row.rowType();
        curTable = curRowType.userTable();
        curNeedsFullName = (tablesNeedingFullName != null) && tablesNeedingFullName.contains(curTable);
    }

    @Override
    public int getRowDepth() {
        return curTable.getDepth();
    }

    @Override
    public String getRowName() {
        return curNeedsFullName ? curTable.getName().toString() : curTable.getName().getTableName();
    }

    @Override
    public boolean isSameRowType() {
        return curRowType == openTypes[getRowDepth()];
    }

    @Override
    public void pushRowType() {
        openTypes[getRowDepth()] = curRowType;
    }
}
