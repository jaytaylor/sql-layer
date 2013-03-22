
package com.akiban.server.aggregation;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

public interface Aggregator {
    void input(ValueSource input);
    void output(ValueTarget output);
    ValueSource emptyValue();
}
