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

import static com.foundationdb.sql.optimizer.rule.cost.PersistitCostModelMeasurements.*;

public class PersistitCostModel extends CostModel
{
    public PersistitCostModel(Schema schema, TableRowCounts tableRowCounts) {
        super(schema, tableRowCounts);
    }

    @Override
    protected double treeScan(int rowWidth, long nRows) {
        return
            RANDOM_ACCESS_PER_ROW + RANDOM_ACCESS_PER_BYTE * rowWidth +
            nRows * (SEQUENTIAL_ACCESS_PER_ROW + SEQUENTIAL_ACCESS_PER_BYTE * rowWidth);
    }
}
