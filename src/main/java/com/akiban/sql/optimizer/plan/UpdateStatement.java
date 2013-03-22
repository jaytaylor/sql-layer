
package com.akiban.sql.optimizer.plan;

import java.util.List;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.ColumnContainer;

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
