
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** A condition evaluated against a set of rows.
 */
public class AnyCondition extends SubqueryExpression implements ConditionExpression
{
    public AnyCondition(Subquery subquery, 
                        DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(subquery, sqlType, sqlSource);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AnyCondition)) return false;
        AnyCondition other = (AnyCondition)obj;
        // Currently this is ==; don't match whole subquery.
        return getSubquery().equals(other.getSubquery());
    }

    @Override
    public int hashCode() {
        return getSubquery().hashCode();
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

    @Override
    public String toString() {
        return "ANY(" + super.toString() + ")";
    }

}
