
package com.akiban.sql.optimizer.plan;

/** A Boolean expression that can be used as a condition clause.
 */
public interface ConditionExpression extends ExpressionNode
{
    public static enum Implementation {
        NORMAL, INDEX, GROUP_JOIN, POTENTIAL_GROUP_JOIN
    }

    public Implementation getImplementation();
}
