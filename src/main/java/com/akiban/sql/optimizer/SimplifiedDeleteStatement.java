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

package com.akiban.sql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.akiban.ais.model.Column;
import com.akiban.sql.parser.DeleteNode;
import com.akiban.sql.parser.ValueNode;

/**
 * An SQL DELETE statement turned into a simpler form for the interim
 * heuristic optimizer.
 */
public class SimplifiedDeleteStatement extends SimplifiedTableStatement
{
    private List<SimpleSelectColumn> queryColumns = null;
    private List<TargetColumn> targetColumns = null;
    
    public SimplifiedDeleteStatement(DeleteNode delete, Set<ValueNode> joinConditions) {
        super(delete, joinConditions);
        
        int table_cols = getTargetTable().getTable().getColumnsIncludingInternal().size();
        
        queryColumns = new ArrayList<SimpleSelectColumn>(table_cols);
        targetColumns = new ArrayList<TargetColumn>(table_cols);
        
        for (Column column : getTargetTable().getTable().getColumnsIncludingInternal()) {
            ColumnExpression expr = getColumnExpression(column);
            SimpleSelectColumn selectCol = new SimpleSelectColumn(column.getName(), true,
                    expr,
                    null);
            queryColumns.add(selectCol);
            expr.getTable().addSelectColumn(selectCol);
            targetColumns.add(new TargetColumn(column, getColumnExpression(column)));
        }
    }
    /**
     * Override the select columns to turn the query into a 
     * SELECT * FROM <target table>. 
     */
    @Override
    public List<SimpleSelectColumn> getSelectColumns() {
        return queryColumns;
    }

    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        str.append("\ntarget: ");
        str.append(getTargetTable());
        if (!getConditions().isEmpty()) {
            str.append("\nconditions: ");
            for (int i = 0; i < getConditions().size(); i++) {
                if (i > 0) str.append(",\n  ");
                str.append(getConditions().get(i));
            }
        }
        return str.toString();
    }

    @Override
    public List<TargetColumn> getTargetColumns() {
        return targetColumns;
    }

    @Override
    public ColumnExpressionToIndex getFieldOffset() {
        return null;
    }

}
