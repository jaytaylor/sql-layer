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
package com.akiban.server.aggregation;

import com.akiban.qp.rowtype.RowType;

import java.util.ArrayList;
import java.util.List;

public final class Aggregators {

    public static List<AggregatorId> aggregatorIds(List<String> names, RowType rowType, int rowTypeOffset) {
        // TODO input validations
        List<AggregatorId> result = new ArrayList<AggregatorId>();
        for (String name : names) {
            result.add(new AggregatorId(name, rowType.typeAt(rowTypeOffset++)));
        }
        return result;
    }

    public static List<AggregatorFactory> factories(AggregatorRegistry registry, List<AggregatorId> aggregatorIds) {
        List<AggregatorFactory> result = new ArrayList<AggregatorFactory>();
        for (AggregatorId id : aggregatorIds) {
            result.add(registry.get(id.name(), id.type()));
        }
        return result;
    }

}
