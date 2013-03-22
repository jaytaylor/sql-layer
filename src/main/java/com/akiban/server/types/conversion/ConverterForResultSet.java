
package com.akiban.server.types.conversion;

import com.akiban.qp.operator.Cursor;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.Extractors;

final class ConverterForResultSet extends ObjectConverter<Cursor> {

    static final ObjectConverter<Cursor> INSTANCE = new ConverterForResultSet();

    @Override
    protected void putObject(ValueTarget target, Cursor value) {
        target.putResultSet(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.RESULT_SET;
    }

    private ConverterForResultSet() {
        super(Extractors.getCursorExtractor());
    }
}
