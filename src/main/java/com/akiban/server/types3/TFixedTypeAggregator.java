
package com.akiban.server.types3;

public abstract class TFixedTypeAggregator extends TAggregatorBase {
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputClass());
    }

    public TFixedTypeAggregator(String name, TClass inputAndOutput) {
        super(name, inputAndOutput);
    }
}
