
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An expression evaluated by a subquery.
 */
public abstract class SubqueryExpression extends BaseExpression 
{
    private Subquery subquery;

    public SubqueryExpression(Subquery subquery, 
                              DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.subquery = subquery;
    }

    public Subquery getSubquery() {
        return subquery;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof PlanVisitor)
                subquery.accept((PlanVisitor)v);
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this) return result;
        }
        if (v instanceof PlanVisitor)
          subquery.accept((PlanVisitor)v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        return subquery.summaryString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subquery = (Subquery)subquery.duplicate(map);
    }

}
