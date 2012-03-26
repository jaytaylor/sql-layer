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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Column;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;

import java.util.*;

/** A SQL UPDATE statement. */
public class UpdateStatement extends BaseUpdateStatement
{
    /** One of the SET clauses of an UPDATE statement.
     */
    public static class UpdateColumn extends AnnotatedExpression {
        private Column column;

        public UpdateColumn(Column column, ExpressionNode value) {
            super(value);
            this.column = column;
        }

        public Column getColumn() {
            return column;
        }

        @Override
        public String toString() {
            return column + " = " + getExpression();
        }
    }

    private List<UpdateColumn> updateColumns;

    public UpdateStatement(PlanNode query, TableNode targetTable,
                           List<UpdateColumn> updateColumns,
                           EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query, targetTable, columnEquivalencies);
        this.updateColumns = updateColumns;
    }

    public List<UpdateColumn> getUpdateColumns() {
        return updateColumns;
    }

    @Override
    public String summaryString() {
        return super.summaryString() + "(" + getTargetTable() + updateColumns + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        updateColumns = duplicateList(updateColumns, map);
    }

}
