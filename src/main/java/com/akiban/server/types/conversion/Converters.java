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

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import static com.akiban.server.types.AkType.*;

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
            if (conversionType == null || conversionType == UNSUPPORTED || conversionType == NULL) {
                throw new InconvertibleTypesException(source.getConversionType(), conversionType);
            }
            get(conversionType).convert(source, target);
        }
        return target;
    }

    public static boolean isConversionAllowed(AkType sourceType, AkType targetType) {
        // NULL -> * is always allowed, UNSUPPORTED -> * is always disallowed, else A -> A is allowed
        if (sourceType == AkType.NULL || (sourceType != AkType.UNSUPPORTED && sourceType.equals(targetType))) {
            return true;
        }
        Set<AkType> allowedTargets = readOnlyLegalConversions.get(sourceType);
        return allowedTargets != null && allowedTargets.contains(targetType);
    }

    // for use in this class
    
    private static AbstractConverter get(AkType type) {
        return readOnlyConvertersMap.get(type);
    }

    private static Map<AkType,AbstractConverter> createConvertersMap() {
        Map<AkType,AbstractConverter> result = new EnumMap<AkType, AbstractConverter>(AkType.class);
        result.put(DATE, LongConverter.DATE);
        result.put(DATETIME, LongConverter.DATETIME);
        result.put(DECIMAL, ConverterForBigDecimal.INSTANCE);
        result.put(DOUBLE, ConverterForDouble.SIGNED);
        result.put(FLOAT, ConverterForFloat.SIGNED);
        result.put(INT, LongConverter.INT);
        result.put(LONG, LongConverter.LONG);
        result.put(VARCHAR, ConverterForString.STRING);
        result.put(TEXT, ConverterForString.TEXT);
        result.put(TIME, LongConverter.TIME);
        result.put(TIMESTAMP, LongConverter.TIMESTAMP);
        result.put(INTERVAL_MILLIS, LongConverter.INTERVAL_MILLIS);
        result.put(INTERVAL_MONTH,LongConverter.INTERVAL_MONTH);
        result.put(U_BIGINT, ConverterForBigInteger.INSTANCE);
        result.put(U_DOUBLE, ConverterForDouble.UNSIGNED);
        result.put(U_FLOAT, ConverterForFloat.UNSIGNED);
        result.put(U_INT, LongConverter.U_INT);
        result.put(VARBINARY, ConverterForVarBinary.INSTANCE);
        result.put(YEAR, LongConverter.YEAR);
        result.put(BOOL, new ConverterForBool());
        result.put(RESULT_SET, ConverterForResultSet.INSTANCE);
        return result;
    }

    private static Map<AkType,Set<AkType>> createLegalConversionsMap() {
        
        ConversionsBuilder builder = new ConversionsBuilder(NULL, UNSUPPORTED);
        
        builder.alias(VARCHAR, TEXT);
        builder.alias(DOUBLE, U_DOUBLE);
        builder.alias(FLOAT, U_FLOAT);
        builder.alias(LONG, INT);
        builder.alias(LONG, U_INT);
        
        builder.legalConversions(VARCHAR,
                BOOL,
                DOUBLE,
                FLOAT,
                LONG,
                U_BIGINT,
                TIME,
                TIMESTAMP,
                INTERVAL_MILLIS,
                INTERVAL_MONTH,
                YEAR,
                DATE,
                DATETIME,
                DECIMAL,
                VARBINARY
        );
        builder.legalConversions(U_BIGINT,
                FLOAT,
                DOUBLE,
                DECIMAL,
                DOUBLE,
                VARCHAR,
                LONG,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(DECIMAL,
                U_BIGINT,
                VARCHAR,
                LONG,
                INTERVAL_MILLIS,
                INTERVAL_MONTH,
                FLOAT,
                DOUBLE
        );
        builder.legalConversions(DOUBLE,
                FLOAT,
                DECIMAL,
                LONG,
                U_BIGINT,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(FLOAT,
                DOUBLE,
                DECIMAL,
                LONG,
                U_BIGINT,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(LONG,
                DOUBLE,
                FLOAT,
                U_BIGINT,
                DECIMAL,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH
        );
        builder.legalConversions(DATE,
                VARCHAR,
                LONG
        );
        builder.legalConversions(DATETIME,
                VARCHAR,
                LONG
        );
        builder.legalConversions(TIME,
                VARCHAR,
                LONG
        );
        builder.legalConversions(TIMESTAMP,
                VARCHAR,
                LONG
        );
        builder.legalConversions(YEAR,
                VARCHAR,
                LONG
        );
        builder.legalConversions(BOOL,
                VARCHAR,
                INTERVAL_MILLIS,
                INTERVAL_MONTH,
                LONG,
                DOUBLE,
                FLOAT,
                DECIMAL,
                U_BIGINT,
                DATE,
                TIME,
                DATETIME,
                TIMESTAMP,
                YEAR
        );

        builder.legalConversions(INTERVAL_MILLIS,
                DOUBLE,
                DECIMAL,
                U_BIGINT,                
                LONG,
                VARCHAR               
        );

        builder.legalConversions(INTERVAL_MONTH,
                DOUBLE,
                DECIMAL,
                U_BIGINT,
                LONG,
                VARCHAR
        );
        
        return builder.result();
    }
    
    private Converters() {}
    
    // class state
    /**
     * A mapping of AkTypes to converters. This map must never be modified once it's created -- the instance's
     * thread safety comes solely from the happens-before relationship of the class instantiation.
     */
    private static final Map<AkType,AbstractConverter> readOnlyConvertersMap = createConvertersMap();

    /**
     * Mapping of each source AkType to the set of AkTypes it can be converted to. Unsynchronized, so read only.
     */
    private static final Map<AkType,Set<AkType>> readOnlyLegalConversions = createLegalConversionsMap();
}
