
package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueTarget;

abstract class ConverterForDouble extends DoubleConverter {

    static final DoubleConverter SIGNED = new ConverterForDouble() {
        @Override
        protected void putDouble(ValueTarget target, double value) {
            target.putDouble(value);
        }
    };

    static final DoubleConverter UNSIGNED = new ConverterForDouble() {
        @Override
        protected void putDouble(ValueTarget target, double value) {
            target.putUDouble(value);
        }
    };

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.DOUBLE;
    }

    private ConverterForDouble() {}
}
