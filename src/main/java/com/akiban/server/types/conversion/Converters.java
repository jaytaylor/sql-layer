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

package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

import java.util.EnumMap;
import java.util.Map;

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
    public static <T extends ValueTarget> T convert(ValueSource source, T target) {
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

    // for use in this class
    
    private static AbstractConverter get(AkType type) {
        return readOnlyConvertersMap.get(type);
    }

    private static Map<AkType,AbstractConverter> createConvertersMap() {
        Map<AkType,AbstractConverter> result = new EnumMap<AkType, AbstractConverter>(AkType.class);
        result.put(AkType.DATE, ConvertersForDates.DATE);
        result.put(AkType.DATETIME, ConvertersForDates.DATETIME);
        result.put(AkType.DECIMAL, ConverterForBigDecimal.INSTANCE);
        result.put(AkType.DOUBLE, ConverterForDouble.SIGNED);
        result.put(AkType.FLOAT, ConverterForFloat.SIGNED);
        result.put(AkType.INT, ConverterForLong.INT);
        result.put(AkType.LONG, ConverterForLong.LONG);
        result.put(AkType.VARCHAR, ConverterForString.STRING);
        result.put(AkType.TEXT, ConverterForString.TEXT);
        result.put(AkType.TIME, ConvertersForDates.TIME);
        result.put(AkType.TIMESTAMP, ConvertersForDates.TIMESTAMP);
        result.put(AkType.U_BIGINT, ConverterForBigInteger.INSTANCE);
        result.put(AkType.U_DOUBLE, ConverterForDouble.UNSIGNED);
        result.put(AkType.U_FLOAT, ConverterForFloat.UNSIGNED);
        result.put(AkType.U_INT, ConverterForLong.U_INT);
        result.put(AkType.VARBINARY, ConverterForVarBinary.INSTANCE);
        result.put(AkType.YEAR, ConvertersForDates.YEAR);
        return result;
    }
    
    private Converters() {}
    
    // class state
    /**
     * A mapping of AkTypes to converters. This map must never be modified once it's created -- the instance's
     * thread safety comes solely from the happens-before relationship of the class instantiation.
     */
    private static final Map<AkType,AbstractConverter> readOnlyConvertersMap = createConvertersMap();
}
