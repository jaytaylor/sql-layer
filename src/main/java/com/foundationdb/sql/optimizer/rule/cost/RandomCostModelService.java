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
import com.foundationdb.server.service.Service;

public class RandomCostModelService implements CostModelFactory, Service
{
    @Override
    public CostModel newCostModel(Schema schema, TableRowCounts tableRowCounts) {
        String seedStr = System.getProperty("fdbsql.test.seed");
        if(seedStr == null) {
            this.seed = System.currentTimeMillis();
        } else {
            this.seed = Long.parseLong(seedStr);
        }
        return new RandomCostModel(schema, tableRowCounts, seed);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
        stop();
    }

    public long getSeed(){
        return seed;
    }

    private long seed;
}
