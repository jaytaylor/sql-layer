
package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.Extractors;

import java.math.BigDecimal;

final class ConverterForBigDecimal extends ObjectConverter<BigDecimal> {

    static final ObjectConverter<BigDecimal> INSTANCE = new ConverterForBigDecimal();

    @Override
    protected void putObject(ValueTarget target, BigDecimal value) {
        target.putDecimal(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.DECIMAL;
    }

    private ConverterForBigDecimal() {
        super(Extractors.getDecimalExtractor());
    }
}
