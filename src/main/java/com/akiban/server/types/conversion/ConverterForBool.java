
package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.BooleanExtractor;
import com.akiban.server.types.extract.Extractors;

public final class ConverterForBool extends AbstractConverter {
    public static final ConverterForBool INSTANCE = new ConverterForBool();

    @Override
    protected void doConvert(ValueSource source, ValueTarget target) {
        target.putBool(extractor.getBoolean(source, null));
    }

    @Override
    protected AkType targetConversionType() {
        return AkType.BOOL;
    }

    private final BooleanExtractor extractor = Extractors.getBooleanExtractor();
}
