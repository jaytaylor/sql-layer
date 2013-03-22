
package com.akiban.server.types.extract;

import com.akiban.qp.operator.Cursor;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

final class ExtractorForResultSet extends ObjectExtractor<Cursor> {

    @Override
    public Cursor getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case RESULT_SET:   return source.getResultSet();
        default: throw unsupportedConversion(type);
        }
    }

    @Override
    public Cursor getObject(String string) {
        throw new UnsupportedOperationException();
    }

    ExtractorForResultSet() {
        super(AkType.RESULT_SET);
    }
}
