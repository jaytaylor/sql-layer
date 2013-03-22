
package com.akiban.server.t3expressions;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TKeyComparable;
import com.akiban.server.types3.texpressions.TValidatedAggregator;
import com.akiban.server.types3.texpressions.TValidatedScalar;

public interface T3RegistryService {
    OverloadResolver<TValidatedScalar> getScalarsResolver();
    OverloadResolver<TValidatedAggregator> getAggregatesResolver();
    TCastResolver getCastsResolver();
    TKeyComparable getKeyComparable(TClass left, TClass right);
    enum FunctionKind { SCALAR, AGGREGATE };
    FunctionKind getFunctionKind(String name);
}
