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

package com.akiban.sql.pg;

import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;

public class PostgresParameterDecoder
{
    private FromObjectValueSource stringSource;
    private ToObjectValueTarget objectTarget;
    
    public PostgresParameterDecoder() {
    }

    public Object decodeParameter(String value, PostgresType type) {
        if (value == null)
            return null;
        if (type == null)
            return value;
        AkType akType = type.getAkType();
        if (akType == AkType.VARCHAR)
            return value;
        if (stringSource == null) {
            stringSource = new FromObjectValueSource();
            objectTarget = new ToObjectValueTarget();
        }
        stringSource.setReflectively(value);
        objectTarget.expectType(akType);
        return Converters.convert(stringSource, objectTarget).lastConvertedValue();
    }
}
