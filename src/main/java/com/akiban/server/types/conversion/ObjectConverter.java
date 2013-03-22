
package com.akiban.server.types.conversion;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.ObjectExtractor;

abstract class ObjectConverter<T> extends AbstractConverter {

    // defined in subclasses

    protected abstract void putObject(ValueTarget target, T value);

    // for use in this package

    @Override
    protected final void doConvert(ValueSource source, ValueTarget target) {
        putObject(target, extractor.getObject(source));
    }

    ObjectConverter(ObjectExtractor<? extends T> extractor) {
        this.extractor = extractor;
    }

    private final ObjectExtractor<? extends T> extractor;
}
