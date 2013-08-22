/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.extract;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.util.ByteSource;
import com.foundationdb.util.WrappingByteSource;
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
