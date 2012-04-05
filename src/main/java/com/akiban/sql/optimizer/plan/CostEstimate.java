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

package com.akiban.sql.optimizer.plan;

/** The basic unit of costing. Keep tracks of a number of rows that
 * result and the total cost to get them there. */
// TODO: Should this go in a qp package and also be attached to
// operators after we're done?
public class CostEstimate implements Comparable<CostEstimate>
{
    private final long rowCount;
    private final double cost;

    public CostEstimate(long rowCount, double cost) {
        this.rowCount = rowCount;
        this.cost = cost;
    }

    public long getRowCount() {
        return rowCount;
    }
    public double getCost() {
        return cost;
    }

    public int compareTo(CostEstimate other) {
        return Double.compare(cost, other.cost);
    }

    /** Cost of one operation after the other. */
    public CostEstimate sequence(CostEstimate next) {
        return new CostEstimate(next.rowCount, cost + next.cost);
    }

    /** Cost of one operation combined with another. */
    public CostEstimate union(CostEstimate other) {
        return new CostEstimate(rowCount + other.rowCount, cost + other.cost);
    }

    /** Cost of operation repeated. */
    public CostEstimate repeat(long count) {
        return new CostEstimate(rowCount * count, cost * count);
    }

    /** Cost of one operation nested within another. */
    public CostEstimate nest(CostEstimate inner) {
        return new CostEstimate(rowCount * inner.rowCount,
                                cost + rowCount * inner.cost);
    }

    @Override
    public String toString() {
        return String.format("rows = %d, cost = %g", rowCount, cost);
    }
}
