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
package com.akiban.server.aggregation.std;

import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.aggregation.AggregatorId;
import com.akiban.server.aggregation.AggregatorRegistry;
import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.types.AkType;

public final class StandardAggregatorRegistry implements AggregatorRegistry {
    @Override
    public AggregatorFactory get(AggregatorId aggregatorId) {
        if ("MIN".equals(aggregatorId.name()))
            return LongAggregators.min(aggregatorId.type());
        else if ("MAX".equals(aggregatorId.name()))
            return LongAggregators.max(aggregatorId.type());
        throw new NoSuchFunctionException(aggregatorId.name());
    }
}
