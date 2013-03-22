
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An EXISTS subqyery.
 */
public class ExistsCondition extends SubqueryExpression implements ConditionExpression
{
    public ExistsCondition(Subquery subquery, 
                           DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(subquery, sqlType, sqlSource);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ExistsCondition)) return false;
        ExistsCondition other = (ExistsCondition)obj;
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
        return "EXISTS(" + super.toString() + ")";
    }

}
