
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An expression representing a nested result set.
 */
public class SubqueryResultSetExpression extends SubqueryExpression 
{
    public SubqueryResultSetExpression(Subquery subquery, 
                                       DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(subquery, sqlType, sqlSource);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SubqueryResultSetExpression)) return false;
        SubqueryResultSetExpression other = (SubqueryResultSetExpression)obj;
        // Currently this is ==; don't match whole subquery.
        return getSubquery().equals(other.getSubquery());
    }

    @Override
    public int hashCode() {
        return getSubquery().hashCode();
    }

}
