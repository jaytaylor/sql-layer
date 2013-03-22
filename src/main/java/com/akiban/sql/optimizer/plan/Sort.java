
package com.akiban.sql.optimizer.plan;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.CharacterTypeAttributes;

import java.util.List;

/** A key expression in an ORDER BY clause. */
public class Sort extends BasePlanWithInput
{
    public static class OrderByExpression extends AnnotatedExpression {
        private boolean ascending;

        public OrderByExpression(ExpressionNode expression, boolean ascending) {
            super(expression);
            this.ascending = ascending;
        }

        public boolean isAscending() {
            return ascending;
        }
        public void setAscending(boolean ascending) {
            this.ascending = ascending;
        }

        public AkCollator getCollator() {
            return getExpression().getCollator();
        }

        public String toString() {
            if (ascending)
                return super.toString();
            else
                return super.toString() + " DESC";
        }
    }

    private List<OrderByExpression> orderBy;

    public Sort(PlanNode input, List<OrderByExpression> orderBy) {
        super(input);
        this.orderBy = orderBy;
    }

    public List<OrderByExpression> getOrderBy() {
        return orderBy;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (OrderByExpression expr : orderBy) {
                        expr.accept((ExpressionRewriteVisitor)v);
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (OrderByExpression expr : orderBy) {
                        if (!expr.getExpression().accept((ExpressionVisitor)v)) {
                            break;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString() {
        return super.summaryString() + orderBy;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        orderBy = duplicateList(orderBy, map);
    }

}
