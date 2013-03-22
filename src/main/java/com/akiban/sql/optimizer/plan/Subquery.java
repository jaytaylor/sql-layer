
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.rule.EquivalenceFinder;

import java.util.Set;

/** A marker node around some subquery.
 */
public class Subquery extends BaseQuery
{
    private Set<ColumnSource> outerTables;

    public Subquery(PlanNode inside, EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(inside, columnEquivalencies);
    }

    @Override
    public Set<ColumnSource> getOuterTables() {
        if (outerTables != null)
            return outerTables;
        else
            return super.getOuterTables();
    }

    public void setOuterTables(Set<ColumnSource> outerTables) {
        this.outerTables = outerTables;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        if (outerTables != null)
            outerTables = duplicateSet(outerTables, map);
    }

}
