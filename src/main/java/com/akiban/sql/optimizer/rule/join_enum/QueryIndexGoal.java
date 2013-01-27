/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.rule.join_enum;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.IndexScan.OrderEffectiveness;
import com.akiban.sql.optimizer.rule.SchemaRulesContext;
import com.akiban.sql.optimizer.rule.cost.CostEstimator;

/** The overall goal of a query: WHERE conditions, ORDER BY, etc. */
public class QueryIndexGoal
{
    private BaseQuery query;
    private SchemaRulesContext rulesContext;
    private ConditionList whereConditions;

    // If both grouping and ordering are present, they must be
    // compatible. Something satisfying the ordering would also handle
    // the grouping. All the order by columns must also be group by
    // columns, though not necessarily in the same order. There can't
    // be any additional order by columns, because even though it
    // would be properly grouped going into aggregation, it wouldn't
    // still be sorted by those coming out. It's hard to write such a
    // query in SQL, since the ORDER BY can't contain columns not in
    // the GROUP BY, and non-columns won't appear in the index.
    private AggregateSource grouping;
    private Sort ordering;
    private Project projectDistinct;
    private Limit limit;

    private TableNode updateTarget;

    public QueryIndexGoal(BaseQuery query,
                          SchemaRulesContext rulesContext,
                          ConditionList whereConditions,
                          AggregateSource grouping,
                          Sort ordering,
                          Project projectDistinct,
                          Limit limit) {
        this.query = query;
        this.rulesContext = rulesContext;
        this.whereConditions = whereConditions;
        this.grouping = grouping;
        this.ordering = ordering;
        this.projectDistinct = projectDistinct;
        this.limit = limit;

        if (query instanceof DMLStatement) {
            DMLStatement stmt = (DMLStatement)query;
            if (stmt.getType() == BaseUpdateStatement.StatementType.DELETE ||
                    stmt.getType() == BaseUpdateStatement.StatementType.UPDATE) {
                updateTarget = stmt.getTargetTable();
            }
        }
    }

    public BaseQuery getQuery() {
        return query;
    }
    public SchemaRulesContext getRulesContext() {
        return rulesContext;
    }
    public CostEstimator getCostEstimator() {
        return rulesContext.getCostEstimator();
    }
    public ConditionList getWhereConditions() {
        return whereConditions;
    }
    public AggregateSource getGrouping() {
        return grouping;
    }
    public Sort getOrdering() {
        return ordering;
    }
    public Project getProjectDistinct() {
        return projectDistinct;
    }
    public TableNode getUpdateTarget() {
        return updateTarget;
    }

    public boolean needSort(OrderEffectiveness orderEffectiveness) {
        if ((ordering != null) ||
            (projectDistinct != null))
            return (orderEffectiveness != IndexScan.OrderEffectiveness.SORTED);
        if (grouping != null)
            return ((orderEffectiveness != IndexScan.OrderEffectiveness.SORTED) &&
                    (orderEffectiveness != IndexScan.OrderEffectiveness.GROUPED));
        return false;
    }

    public int sortFields() {
        if (ordering != null)
            return ordering.getOrderBy().size();
        if (projectDistinct != null)
            return projectDistinct.getFields().size();
        if (grouping != null)
            return grouping.getNGroupBy();
        return 0;
    }

    /** Change GROUP BY, and ORDER BY upstream of <code>node</code> as
     * a consequence of <code>orderEffectiveness</code> being used.
     */
    public void installOrderEffectiveness(OrderEffectiveness orderEffectiveness) {
        if (grouping != null) {
            AggregateSource.Implementation implementation;
            switch (orderEffectiveness) {
            case SORTED:
            case GROUPED:
                implementation = AggregateSource.Implementation.PRESORTED;
                break;
            case PARTIAL_GROUPED:
                implementation = AggregateSource.Implementation.PREAGGREGATE_RESORT;
                break;
            default:
                implementation = AggregateSource.Implementation.SORT;
                break;
            }
            grouping.setImplementation(implementation);
        }
        if (ordering != null) {
            if (orderEffectiveness == IndexScan.OrderEffectiveness.SORTED) {
                // Sort not needed: splice it out.
                ordering.getOutput().replaceInput(ordering, ordering.getInput());
            }
        }
        if (projectDistinct != null) {
            Distinct distinct = (Distinct)projectDistinct.getOutput();
            Distinct.Implementation implementation;
            switch (orderEffectiveness) {
            case SORTED:
                implementation = Distinct.Implementation.PRESORTED;
                break;
            default:
                implementation = Distinct.Implementation.SORT;
                break;
            }
            distinct.setImplementation(implementation);
        }
    }

    public long getLimit() {
        if ((limit == null) || limit.isOffsetParameter() || limit.isLimitParameter())
            return -1;
        // The number of rows that must be processed before you are done.
        return (limit.getOffset() + limit.getLimit());
    }

}
