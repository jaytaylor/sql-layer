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
 * An SQL UPDATE statement turned into a simpler form for the interim
 * heuristic optimizer.
 */
public class SimplifiedUpdateStatement extends SimplifiedTableStatement
{
    static class UpdateColumn {
        private Column column;
        private SimpleExpression value;

        public UpdateColumn(Column column, SimpleExpression value) {
            this.column = column;
            this.value = value;
        }

        public Column getColumn() {
            return column;
        }
        public SimpleExpression getValue() {
            return value;
        }
    }

    private List<UpdateColumn> updateColumns;

    public SimplifiedUpdateStatement(UpdateNode update, Set<ValueNode> joinConditions)
            throws StandardException {
        super(update, joinConditions);
    }
    
    public List<UpdateColumn> getUpdateColumns() {
        return updateColumns;
    }

    @Override
    protected void fillFromSelect(SelectNode select, Set<ValueNode> joinConditions) 
            throws StandardException {
        super.fillFromSelect(select, joinConditions);
        fillUpdateColumns(select.getResultColumns());
    }

    protected void fillUpdateColumns(ResultColumnList resultColumns)
            throws StandardException {
        updateColumns = new ArrayList<UpdateColumn>(resultColumns.size());
        for (ResultColumn result : resultColumns) {
            Column column = getColumnReferenceColumn(result.getReference(),
                                                     "Unsupported result column");
            SimpleExpression value = getSimpleExpression(result.getExpression());
            updateColumns.add(new UpdateColumn(column, value));
        }
    }
    
}
