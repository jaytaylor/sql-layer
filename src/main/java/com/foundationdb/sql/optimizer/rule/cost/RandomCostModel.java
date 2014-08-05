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

package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.sql.optimizer.plan.CostEstimate;

import java.util.Random;

public class RandomCostModel extends CostModel
{
    // TODO: Need some kind of external control of Random seed, so
    // that this is reproducible when problems are detected. Should it
    // reset every query?

    private final Random random;

    public RandomCostModel(Schema schema, TableRowCounts tableRowCounts, long seed) {
        super(schema, tableRowCounts);
        random = new Random(seed);
    }

    @Override
    protected double treeScan(int rowWidth, long nRows) {
        return 10 + nRows * .5;
    }

    public synchronized CostEstimate adjustCostEstimate(CostEstimate costEstimate) {
        return new CostEstimate(costEstimate.getRowCount(),
                                random.nextDouble() * costEstimate.getCost());
    }
}
