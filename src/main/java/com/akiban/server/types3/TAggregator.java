
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public interface TAggregator extends TOverload {
    void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object option);
    void emptyValue(PValueTarget state);
//    TInstance resultType(TPreptimeValue value);
//    TClass getTypeClass();
//    String name();
}
