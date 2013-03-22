
package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueTarget;

abstract class ConverterForFloat extends FloatConverter {

    static final FloatConverter SIGNED = new ConverterForFloat() {
        @Override
        protected void putFloat(ValueTarget target, float value) {
            target.putFloat(value);
        }
    };

    static final FloatConverter UNSIGNED = new ConverterForFloat() {
        @Override
        protected void putFloat(ValueTarget target, float value) {
            target.putUFloat(value);
        }
    };

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.FLOAT;
    }

    private ConverterForFloat() {}
}
