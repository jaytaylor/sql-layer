
package com.akiban.server.aggregation;

import com.akiban.server.types.AkType;

public interface AggregatorRegistry {
    AggregatorFactory get(String name, AkType type);
}
