
package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.Extractors;

import java.math.BigInteger;

final class ConverterForBigInteger extends ObjectConverter<BigInteger> {

    static final ObjectConverter<BigInteger> INSTANCE = new ConverterForBigInteger();

    @Override
    protected void putObject(ValueTarget target, BigInteger value) {
        target.putUBigInt(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.U_BIGINT;
    }

    private ConverterForBigInteger() {
        super(Extractors.getUBigIntExtractor());
    }
}
