
package com.akiban.server.types.extract;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.util.ArgumentValidation;

public abstract class AbstractExtractor {

    public final AkType targetConversionType() {
        return targetConversionType;
    }
    // for use by subclasses

    protected InvalidOperationException unsupportedConversion(AkType sourceType) {
        throw new InconvertibleTypesException(sourceType, targetConversionType());
    }

    @Override
    public String toString() {
        return '(' + targetConversionType.name() + " Extractor)";
    }

    // for use in this package

    AbstractExtractor(AkType targetConversionType) {
        ArgumentValidation.notNull("target conversion type", targetConversionType);
        this.targetConversionType = targetConversionType;
    }

    private final AkType targetConversionType;
}
