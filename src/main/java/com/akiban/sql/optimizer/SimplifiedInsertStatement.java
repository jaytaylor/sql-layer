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

        fillTargetColumns(insert.getTargetColumnList());
    }

    public List<Column> getTargetColumns() {
        return targetColumns;
    }

    protected void fillTargetColumns(ResultColumnList rcl) 
            throws StandardException {
        targetColumns = new ArrayList<Column>(targetColumns.size());
        for (ResultColumn resultColumn : rcl) {
            Column column = getColumnReferenceColumn(resultColumn.getExpression(),
                                                     "Unsupported target column");
            targetColumns.add(column);
        }
    }

    public String toString() {
        return super.toString() + 
            "\ncolumns = " + targetColumns;
    }

}
