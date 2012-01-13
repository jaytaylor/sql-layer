/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

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

    /** Cost of operation repeated. */
    public CostEstimate repeat(long count) {
        return new CostEstimate(rowCount * count, cost * count);
    }

    /** Cost of one operation nested within another. */
    public CostEstimate nest(CostEstimate inner) {
        return new CostEstimate(rowCount * inner.rowCount,
                                cost + rowCount * inner.cost);
    }
}
