/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.types.extract;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.util.ByteSource;

final class ExtractorForVarBinary extends ObjectExtractor<ByteSource> {

    @Override
    public ByteSource getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case VARBINARY:   return source.getVarBinary();
        default: throw unsupportedConversion(type);
        }
    }

    @Override
    public ByteSource getObject(String string) {
        throw new UnsupportedOperationException(); // TODO later
    }

    ExtractorForVarBinary() {
        super(AkType.VARBINARY);
    }
}
