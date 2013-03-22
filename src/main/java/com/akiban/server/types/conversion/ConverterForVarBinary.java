
package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.Extractors;
import com.akiban.util.ByteSource;

final class ConverterForVarBinary extends ObjectConverter<ByteSource> {

    static final ObjectConverter<ByteSource> INSTANCE = new ConverterForVarBinary();

    @Override
    protected void putObject(ValueTarget target, ByteSource value) {
        target.putVarBinary(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.VARBINARY;
    }

    private ConverterForVarBinary() {
        super(Extractors.getByteSourceExtractor());
    }
}
