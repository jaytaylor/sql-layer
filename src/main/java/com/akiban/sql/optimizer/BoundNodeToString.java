
package com.akiban.sql.optimizer;

import com.akiban.sql.unparser.NodeToString;
import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.Table;

/**
 * A version of NodeToString that shows qualified names after binding.
 */
public class BoundNodeToString extends NodeToString {
    private boolean useBindings = false;

    public boolean isUseBindings() {
        return useBindings;
    }
    public void setUseBindings(boolean useBindings) {
        this.useBindings = useBindings;
    }

    protected String tableName(TableName node) throws StandardException {
        if (useBindings) {
            Table table = (Table)node.getUserData();
            if (null != table)
                return table.getName().toString();
        }
        return node.getFullTableName();
    }

    protected String columnReference(ColumnReference node) throws StandardException {
        if (useBindings) {
            ColumnBinding columnBinding = (ColumnBinding)node.getUserData();
            if (columnBinding != null) {
                FromTable fromTable = columnBinding.getFromTable();
                return ((fromTable.getCorrelationName() != null) ?
                        fromTable.getCorrelationName() :
                        toString(fromTable.getTableName())) +
                    "." + node.getColumnName();
            }
        }
        return node.getSQLColumnName();
    }
}
