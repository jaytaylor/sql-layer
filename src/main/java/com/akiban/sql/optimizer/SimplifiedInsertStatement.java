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

import java.util.List;
import java.util.Set;

import com.akiban.server.error.InsertNullCheckFailedException;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.sql.parser.InsertNode;
import com.akiban.sql.parser.ValueNode;


import com.akiban.ais.model.Column;

/**
 * An SQL INSERT statement turned into a simpler form for the interim
 * heuristic optimizer.
 */
public class SimplifiedInsertStatement extends SimplifiedTableStatement
{
    private List<TargetColumn> targetColumns;
    
    public SimplifiedInsertStatement(InsertNode insert, Set<ValueNode> joinConditions) {
        super(insert, joinConditions);
        
        // if the insert statement has a target column list (supplied by the user)
        // this gives to order of columns in the select/values list
        if (insert.getTargetColumnList() != null) {
            //fillTargetFromList (insert.getTargetColumnList());
        }
        // This insert statement has no target column list, 
        // meaning the supplied columns are in order of table creation. 
        else {
            //fillTargetFromDDL ();
        }

/*        
        // Set the list of target columns. There are three cases here: 
        // 1) insert supplies list of columns, which is in insert.getTargetColumnList()
        // e.g. INSERT INTO TABLE (c1,c3,c3)...
        if (insert.getTargetColumnList() != null) {
            fillTargetColumns (insert.getTargetColumnList());
        }
        // 2) insert is supplying values which are expected to be 
        // table in column order. 
        // e.g. INSERT INTO Table VALUES (1,2,3) 
        else if (getValues() != null) {
            int ncols = insert.getResultSetNode().getResultColumns().size();
            List<Column> aisColumns = getTargetTable().getTable().getColumns();
            if (ncols > aisColumns.size()) {
                // TODO: issue warning we're truncating the list of values supplied 
                ncols = aisColumns.size();
            }
            targetColumns = new ArrayList<TargetColumn>(ncols);
            int i = 0;
            for (Column column : aisColumns) {
                if (i < ncols) {
                    targetColumns.add(new TargetColumn (column, new ColumnExpression(getTargetTable(), column)));
                } else if (!column.getNullable()) { 
                    throw new InsertNullCheckFailedException(column);
                }
                i++;
            }
        }
        // 3)  insert is using a query, which are expected to be in column order
        // e.g INSERT INTO TABLE SELECT 1,2,3...
        else {
            fillTargetColumns (insert.getResultSetNode().getResultColumns());
        }
        
        // the insert statement is generating values from explicit values
        // not a select statement. Create the targetColumns list from DDL order
        // list of columns. 
        if (getValues() != null) {
            int ncols = insert.getResultSetNode().getResultColumns().size();
            List<Column> aisColumns = getTargetTable().getTable().getColumns();
            if (ncols > aisColumns.size())
                ncols = aisColumns.size();
            targetColumns = new ArrayList<TargetColumn>(ncols);
            for (int i = 0; i < ncols; i++) {
                targetColumns.add(new TargetColumn(aisColumns.get(i), 
                        getSimpleExpression ()
                        
                        new ColumnExpression(getTargetTable(), aisColumns.get(i))));
            }
        }
*/        
    }

    
/*
    protected void fillTargetColumns(ResultColumnList rcl) {
        targetColumns = new ArrayList<TargetColumn>(rcl.size());
        for (ResultColumn resultColumn : rcl) {
            Column column = getColumnReferenceColumn(resultColumn.getReference(),
                                                     "Insert target column");
            SimpleExpression value = getSimpleExpression(resultColumn.getExpression());
            targetColumns.add(new TargetColumn(column, value));
        }
    }
*/
    public List<TargetColumn> getTargetColumns() {
        return targetColumns;
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
