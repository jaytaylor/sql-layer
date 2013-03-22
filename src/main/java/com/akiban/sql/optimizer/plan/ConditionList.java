
package com.akiban.sql.optimizer.plan;

import java.util.ArrayList;
import java.util.Collection;

/** A conjunction of boolean conditions used for WHERE / HAVING / ON / ...
 */
public class ConditionList extends ArrayList<ConditionExpression>
{
    public ConditionList() {
        super();
    }

    public ConditionList(int size) {
        super(size);
    }

    public ConditionList(Collection<? extends ConditionExpression> list) {
        super(list);
    }

    public boolean accept(ExpressionVisitor v) {
        for (ConditionExpression condition : this) {
            if (!condition.accept(v)) {
                return false;
            }
        }
        return true;
    }

    public void accept(ExpressionRewriteVisitor v) {
        for (int i = 0; i < size(); i++) {
            set(i, (ConditionExpression)get(i).accept(v));
        }
    }

    public ConditionList duplicate(DuplicateMap map) {
        ConditionList copy = new ConditionList(size());
        for (ConditionExpression cond : this) {
            copy.add((ConditionExpression)cond.duplicate(map));
        }
        return copy;
    }

}
