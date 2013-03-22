
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.rule.EquivalenceFinder;

/** A top-level SQL SELECT statement. */
public class SelectQuery extends BaseStatement
{
    public SelectQuery(PlanNode query, EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query, columnEquivalencies);
    }

}
