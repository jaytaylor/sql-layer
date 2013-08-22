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

package com.foundationdb.sql.optimizer.plan;

import java.util.List;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ColumnContainer;

/** A SQL UPDATE statement. */
public class UpdateStatement extends BaseUpdateStatement
{
    /** One of the SET clauses of an UPDATE statement.
     */
    public static class UpdateColumn extends AnnotatedExpression implements ColumnContainer {
        private Column column;

        public UpdateColumn(Column column, ExpressionNode value) {
            super(value);
            this.column = column;
        }

        @Override
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
                           TableSource table) {
        super(query, StatementType.UPDATE, targetTable, table);
        this.updateColumns = updateColumns;
    }


    public List<UpdateColumn> getUpdateColumns() {
        return updateColumns;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (UpdateColumn updateColumn : updateColumns) {
                        updateColumn.accept((ExpressionRewriteVisitor)v);
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (UpdateColumn updateColumn : updateColumns) {
                        if (!updateColumn.accept((ExpressionVisitor)v)) {
                            break;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    

    @Override
    protected void fillSummaryString(StringBuilder str) {
        super.fillSummaryString(str);
        str.append(updateColumns);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        updateColumns = duplicateList(updateColumns, map);
    }

}
