
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An expression evaluated by a subquery: first column of first row
 * or <code>NULL</code>.
 */
public class SubqueryValueExpression extends SubqueryExpression 
{
    public SubqueryValueExpression(Subquery subquery, 
                                   DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(subquery, sqlType, sqlSource);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SubqueryValueExpression)) return false;
        SubqueryValueExpression other = (SubqueryValueExpression)obj;
        // Currently this is ==; don't match whole subquery.
        return getSubquery().equals(other.getSubquery());
    }

    @Override
    public int hashCode() {
        return getSubquery().hashCode();
    }

}
