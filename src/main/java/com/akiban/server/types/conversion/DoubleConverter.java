
package com.akiban.server.types.conversion;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.DoubleExtractor;
import com.akiban.server.types.extract.Extractors;

abstract class DoubleConverter extends AbstractConverter {

    // AbstractDoubleConverter interface
    
    // defined in subclasses
    
    protected abstract void putDouble(ValueTarget target, double value);
    
    // for use in this package

    @Override
    protected final void doConvert(ValueSource source, ValueTarget target) {
        putDouble(target, extractor.getDouble(source));
    }

    DoubleConverter() {}

    private final DoubleExtractor extractor = Extractors.getDoubleExtractor();
}
