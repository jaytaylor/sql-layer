
package com.akiban.server.aggregation;

import com.akiban.qp.rowtype.RowType;

import java.util.ArrayList;
import java.util.List;

public final class Aggregators {

    public static List<AggregatorId> aggregatorIds(List<String> names, RowType rowType, int rowTypeOffset) {
        // TODO input validations
        List<AggregatorId> result = new ArrayList<>();
        for (String name : names) {
            result.add(new AggregatorId(name, rowType.typeAt(rowTypeOffset++)));
        }
        return result;
    }

    public static List<AggregatorFactory> factories(AggregatorRegistry registry, List<AggregatorId> aggregatorIds) {
        List<AggregatorFactory> result = new ArrayList<>();
        for (AggregatorId id : aggregatorIds) {
            result.add(registry.get(id.name(), id.type()));
        }
        return result;
    }

}
