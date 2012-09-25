/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.ColumnContainer;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;

import java.util.*;

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
                           EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query, targetTable, null, null, columnEquivalencies);
        this.updateColumns = updateColumns;
    }
    public UpdateStatement(PlanNode query, TableNode targetTable,
            List<UpdateColumn> updateColumns,
                           TableSource table,
                           List<ResultField> results,
            EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query, targetTable, table, results, columnEquivalencies);
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
