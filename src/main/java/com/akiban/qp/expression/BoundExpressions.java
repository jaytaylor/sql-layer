
package com.akiban.qp.expression;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;

public interface BoundExpressions {
    PValueSource pvalue(int index);
    ValueSource eval(int index);
}
