
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.rule.EquivalenceFinder;

import java.util.Collections;
import java.util.Set;

/** A statement / subquery.
 */
public class BaseQuery extends BasePlanWithInput
{
    private EquivalenceFinder<ColumnExpression> columnEquivalencies;
    private CostEstimate costEstimate;

    protected BaseQuery(PlanNode query, EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query);
        this.columnEquivalencies = columnEquivalencies;
    }

    public PlanNode getQuery() {
        return getInput();
    }

    public Set<ColumnSource> getOuterTables() {
        return Collections.<ColumnSource>emptySet();
    }

    public EquivalenceFinder<ColumnExpression> getColumnEquivalencies() {
        return columnEquivalencies;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }
    public void setCostEstimate(CostEstimate costEstimate) {
        this.costEstimate = costEstimate;
    }

}
