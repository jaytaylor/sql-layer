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

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.Column;

import java.util.*;

/**
 * An SQL INSERT statement turned into a simpler form for the interim
 * heuristic optimizer.
 */
public class SimplifiedInsertStatement extends SimplifiedTableStatement
{
    private List<Column> targetColumns;
    
    public SimplifiedInsertStatement(InsertNode insert, Set<ValueNode> joinConditions)
            throws StandardException {
        super(insert, joinConditions);

        if (insert.getTargetColumnList() != null)
            fillTargetColumns(insert.getTargetColumnList());
        else {
            // No explicit column list: use DDL order.
            int ncols = insert.getResultSetNode().getResultColumns().size();
            List<Column> aisColumns = getTargetTable().getTable().getColumns();
            if (ncols > aisColumns.size())
                ncols = aisColumns.size();
            targetColumns = new ArrayList<Column>(ncols);
            for (int i = 0; i < ncols; i++) {
                targetColumns.add(aisColumns.get(i));
            }
        }
    }

    public List<Column> getTargetColumns() {
        return targetColumns;
    }

    protected void fillTargetColumns(ResultColumnList rcl) 
            throws StandardException {
        targetColumns = new ArrayList<Column>(rcl.size());
        for (ResultColumn resultColumn : rcl) {
            Column column = getColumnReferenceColumn(resultColumn.getReference(),
                                                     "Unsupported target column");
            targetColumns.add(column);
        }
    }

    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        str.append("\ntarget: ");
        str.append(getTargetTable());
        str.append("\ncolumns: [");
        for (int i = 0; i < getTargetColumns().size(); i++) {
            if (i > 0) str.append(", ");
            str.append(getTargetColumns().get(i));
        }
        str.append("]");
        str.append("\ngroup: ");
        str.append(getGroup());
        if (getJoins() != null) {
            str.append("\njoins: ");
            str.append(getJoins());
        }
        else if (getValues() != null) {
            str.append("\nvalues: ");
            str.append(getValues());
        }
        if (getSelectColumns() != null) {
            str.append("\nselect: [");
            for (int i = 0; i < getSelectColumns().size(); i++) {
                if (i > 0) str.append(", ");
                str.append(getSelectColumns().get(i));
            }
            str.append("]");
        }
        if (!getConditions().isEmpty()) {
            str.append("\nconditions: ");
            for (int i = 0; i < getConditions().size(); i++) {
                if (i > 0) str.append(",\n  ");
                str.append(getConditions().get(i));
            }
        }
        if (getSortColumns() != null) {
            str.append("\nsort: ");
            for (int i = 0; i < getSortColumns().size(); i++) {
                if (i > 0) str.append(", ");
                str.append(getSortColumns().get(i));
            }
        }
        if (getOffset() > 0) {
            str.append("\noffset: ");
            str.append(getOffset());
        }
        if (getLimit() >= 0) {
            str.append("\nlimit: ");
            str.append(getLimit());
        }
        str.append("\nequivalences: ");
        for (int i = 0; i < getColumnEquivalences().size(); i++) {
            if (i > 0) str.append(",\n  ");
            int j = 0;
            for (Column column : getColumnEquivalences().get(i)) {
                if (j++ > 0) str.append(" = ");
                str.append(column);
            }
        }
        return str.toString();
    }

}
