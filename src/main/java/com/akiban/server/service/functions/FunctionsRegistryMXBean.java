
package com.akiban.server.service.functions;

import com.akiban.server.types.AkType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface FunctionsRegistryMXBean {
    List<String> getScalars();
    List<String> getAggregates();
    Map<String,Set<AkType>> getAggregatesWithTypes();
}
