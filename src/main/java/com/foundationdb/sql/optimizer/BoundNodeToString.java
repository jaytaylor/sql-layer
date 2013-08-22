/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.unparser.NodeToString;
import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;

import com.foundationdb.ais.model.Table;

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
