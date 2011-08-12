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

package com.akiban.server.types;

public final class Converters {

    /**
     * Converts the source into the target. This method takes care of all internal conversions between the source's
     * and target's type. For instance, if the source is pointing at a VARCHAR and the target requires a LONG,
     * but the VARCHAR can be parsed into a Long, this method will take care of that parsing for you.
     * @param source the conversion source
     * @param target the conversion target
     * @param <T> the conversion target's specific type
     * @return the conversion target; this return value is provided as a convenience, so you can chain calls
     */
    public static <T extends ConversionTarget> T convert(ConversionSource source, T target) {
        if (source.isNull()) {
            target.putNull();
        } else {
            AkType conversionType = target.getConversionType();
            get(conversionType).convert(source, target);
        }
        return target;
    }

    public static LongConverter getLongConverter(AkType type) {
        AbstractConverter converter = get(type);
        if (converter instanceof LongConverter)
            return (LongConverter) converter;
        return null;
    }

    private static AbstractConverter get(AkType type) {
        return type.converter();
    }

    // for use in this class
    private Converters() {}
}
