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
