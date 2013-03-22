
package com.akiban.sql.optimizer.plan;

/** Apply a conjunction of Boolean expressions to the input.
 */
public class Select extends BasePlanWithInput
{
    private ConditionList conditions;

    public Select(PlanNode input, ConditionList conditions) {
        super(input);
        this.conditions = conditions;
    }

    public ConditionList getConditions() {
        return conditions;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    conditions.accept((ExpressionRewriteVisitor)v);
                }
                else if (v instanceof ExpressionVisitor) {
                    conditions.accept((ExpressionVisitor)v);
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString() {
        return super.summaryString() + conditions.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        conditions = conditions.duplicate(map);
    }

}
