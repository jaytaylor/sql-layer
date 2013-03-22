
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
