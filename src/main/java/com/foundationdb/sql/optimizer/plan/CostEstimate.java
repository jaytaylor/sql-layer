/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;

/** The basic unit of costing. Keep tracks of a number of rows that
 * result and the total cost to get them there. */
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
