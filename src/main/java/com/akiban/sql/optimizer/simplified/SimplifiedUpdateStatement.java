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

package com.akiban.sql.optimizer.simplified;

import com.akiban.sql.parser.*;

import com.akiban.ais.model.Column;

import java.util.*;

/**
 * An SQL UPDATE statement turned into a simpler form for the interim
 * heuristic optimizer.
 * 
 * TODO: These items are still left to be implemented
 * * CHECK CONSTRAINTS including NOT NULL check as a expression on top of the column ones
 * * DEFAULT VALUES, including AUTO-INCREMENT
 * 
 */
public class SimplifiedUpdateStatement extends SimplifiedTableStatement
{
    private List<TargetColumn> updateColumns;
    private List<SimpleSelectColumn> queryColumns = null;

    public SimplifiedUpdateStatement(UpdateNode update, Set<ValueNode> joinConditions) {
        super(update, joinConditions);
        
        queryColumns = new ArrayList<SimpleSelectColumn>(getTargetTable().getTable().getColumns().size());
        for (Column column : getTargetTable().getTable().getColumns()) {
            ColumnExpression expr = getColumnExpression(column);
            SimpleSelectColumn selectCol = new SimpleSelectColumn(column.getName(), true,
                    expr,
                    null);
            queryColumns.add(selectCol);
            expr.getTable().addSelectColumn(selectCol);
        }
    }
    
    @Override
    public List<TargetColumn> getTargetColumns() {
        return updateColumns;
    }

    @Override
    public ColumnExpressionToIndex getFieldOffset() {
        return null;
    }

    /**
     * Override the select columns to turn the query into a 
     * SELECT * FROM <target table>. 
     */
    @Override
    public List<SimpleSelectColumn> getSelectColumns() {
        return queryColumns;
    }

    @Override
    protected void fillFromSelect(SelectNode select, Set<ValueNode> joinConditions)  {
        super.fillFromSelect(select, joinConditions);
        fillUpdateColumns(select.getResultColumns());
    }

    protected void fillUpdateColumns(ResultColumnList resultColumns) {
        updateColumns = new ArrayList<TargetColumn>(resultColumns.size());
        for (ResultColumn result : resultColumns) {
            Column column = getColumnReferenceColumn(result.getReference(),
                                                     "Update target column");
            SimpleExpression value = getSimpleExpression(result.getExpression());
            updateColumns.add(new TargetColumn(column, value));
        }
    }
    
    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        str.append("\ntarget: ");
        str.append(getTargetTable());
        str.append("\nupdate: [");
        for (int i = 0; i < getTargetColumns().size(); i++) {
            if (i > 0) str.append(", ");
            TargetColumn updateColumn = getTargetColumns().get(i);
            str.append(updateColumn.getColumn());
            str.append(" = ");
            str.append(updateColumn.getValue());
        }
        str.append("]");
        if (!getConditions().isEmpty()) {
            str.append("\nconditions: ");
            for (int i = 0; i < getConditions().size(); i++) {
                if (i > 0) str.append(",\n  ");
                str.append(getConditions().get(i));
            }
        }
        return str.toString();
    }

}
