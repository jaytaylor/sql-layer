
package com.akiban.server.types.extract;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;
import java.io.UnsupportedEncodingException;

final class ExtractorForVarBinary extends ObjectExtractor<ByteSource> {
    
    @Override
    public ByteSource getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case VARBINARY:   return source.getVarBinary();
        case VARCHAR:     return getObject(source.getString());
        case TEXT:        return getObject(source.getText());
        default: throw unsupportedConversion(type);
        }
    }

    @Override
    public ByteSource getObject(String string){
        try
        {
            return new WrappingByteSource(string.getBytes(Converters.DEFAULT_CS));
        } 
        catch (UnsupportedEncodingException ex)
        {
            throw new UnsupportedOperationException(ex);
        }
    }

    ExtractorForVarBinary() {
        super(AkType.VARBINARY);
    }
}
