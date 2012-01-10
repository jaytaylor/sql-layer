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

public class ServerParameterDecoder
{
    private FromObjectValueSource source;
    private ToObjectValueTarget target;
    
    public ServerParameterDecoder() {
    }

    /** Decode the given parameter into a raw object according to the
     * specified type.
     * The object will later be married with the same type by the
     * Variable expression.
     * <code>value</code> is a string for most drivers most of the
     * time, except in the case of a VARBINARY.  Since that is the
     * only target type that will convert from a byte array, an error
     * will be thrown for any other binary value in some server
     * format we don't know.
     */
    public Object decodeParameter(Object value, ServerType type) {
        if (value == null)
            return null;
        if (type == null)
            return value;
        AkType akType = type.getAkType();
        if (akType == AkType.VARCHAR)
            return value;
        if (source == null) {
            source = new FromObjectValueSource();
            target = new ToObjectValueTarget();
        }
        source.setReflectively(value);
        target.expectType(akType);
        return Converters.convert(source, target).lastConvertedValue();
    }
}
