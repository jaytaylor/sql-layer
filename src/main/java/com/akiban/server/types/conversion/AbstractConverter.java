
package com.akiban.server.types.conversion;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

abstract class AbstractConverter {
    public final void convert(ValueSource source, ValueTarget target) {
        if (source.isNull()) {
            target.putNull();
        }
        else {
            doConvert(source, target);
        }
    }

    protected InvalidOperationException unsupportedConversion(AkType sourceType) {
        throw new InconvertibleTypesException(sourceType, targetConversionType());
    }

    protected abstract void doConvert(ValueSource source, ValueTarget target);
    protected abstract AkType targetConversionType();
}
