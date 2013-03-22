
package com.akiban.server.service.functions;

import com.akiban.server.aggregation.AggregatorRegistry;
import com.akiban.server.expression.ExpressionRegistry;

public interface FunctionsRegistry extends AggregatorRegistry, ExpressionRegistry {
    public enum FunctionKind { SCALAR, AGGREGATE };

    public FunctionKind getFunctionKind(String name);
}
