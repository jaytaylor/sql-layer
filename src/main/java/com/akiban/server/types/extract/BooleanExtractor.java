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

public final class BooleanExtractor extends AbstractExtractor {

    // BooleanExtractor interface

    public Boolean getBoolean(ValueSource source, Boolean ifNull) {
        if (source.isNull())
            return ifNull;
        AkType type = source.getConversionType();
        switch (type) {
        case BOOL:      return source.getBool();
        case VARCHAR:   return getBoolean(source.getString());
        case TEXT:      return getBoolean(source.getText());
        default: throw unsupportedConversion(type);
        }
    }

    public boolean getBoolean(String string) {
        return Boolean.valueOf(string);
    }

    public String asString(boolean value) {
        return Boolean.valueOf(value).toString();
    }

    // package-private ctor

    BooleanExtractor() {
        super(AkType.BOOL);
    }
}
