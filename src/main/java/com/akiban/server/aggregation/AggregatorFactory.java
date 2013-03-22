
package com.akiban.server.aggregation;

import com.akiban.server.types.AkType;

public interface AggregatorFactory {
    // Certain functions (GROUP_CONCAT, though there could be more) could have optional 'argument'
    // (Note: 'argument' here doesn't mean 'inputs')
    // For GROUP_CONCAT, it'd be the optional delimeter
    Aggregator get(Object obj);
    Aggregator get();
    String getName();
    AkType outputType();
}
