
package com.akiban.server.types.conversion;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.DoubleExtractor;
import com.akiban.server.types.extract.Extractors;

abstract class FloatConverter extends AbstractConverter {

    // AbstractFloatConverter interface
    
    // defined in subclasses
    
    protected abstract void putFloat(ValueTarget target, float value);
    
    // for use in this package

    @Override
    protected final void doConvert(ValueSource source, ValueTarget target) {
        putFloat(target, (float)extractor.getDouble(source));
    }

    FloatConverter() {}

    private final DoubleExtractor extractor = Extractors.getDoubleExtractor();
}
