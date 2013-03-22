
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.rule.EquivalenceFinder;

/** A top-level (executable) query or statement.
 */
public class BaseStatement extends BaseQuery
{
    protected BaseStatement(PlanNode query, EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query, columnEquivalencies);
    }

}
