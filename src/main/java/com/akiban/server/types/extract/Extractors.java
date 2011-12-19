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

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types.AkType;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.EnumMap;
import java.util.Map;

public final class Extractors {
    // Extractors interface
    public static LongExtractor getLongExtractor(AkType type) {
        return get(type, LongExtractor.class, true);
    }

    public static BooleanExtractor getBooleanExtractor() {
        return BOOLEAN_EXTRACTOR;
    }

    public static DoubleExtractor getDoubleExtractor() {
        return DOUBLE_EXTRACTOR;
    }

    public static ObjectExtractor<String> getStringExtractor() {
        return STRING_EXTRACTOR;
    }

    public static ObjectExtractor<BigInteger> getUBigIntExtractor() {
        return UBIGINT_EXTRACTOR;
    }

    public static ObjectExtractor<BigDecimal> getDecimalExtractor() {
        return DECIMAL_EXTRACTOR;
    }

    public static ObjectExtractor<ByteSource> getByteSourceExtractor() {
        return VARBINARY_EXTRACTOR;
    }

    public static ObjectExtractor<?> getObjectExtractor(AkType type) {
        switch (type) {
        case VARBINARY: return VARBINARY_EXTRACTOR;
        case VARCHAR:   return STRING_EXTRACTOR;
        case DECIMAL:   return DECIMAL_EXTRACTOR;
        case U_BIGINT:  return UBIGINT_EXTRACTOR;
        default: throw new AkibanInternalException("not an ObjectExtractor type: " + type);
        }
    }

    // private methods

    private static <E extends AbstractExtractor> E get(AkType type, Class<E> extractorClass, boolean nullDefault) {
        AbstractExtractor extractor = readOnlyExtractorsMap.get(type);
        if (extractorClass.isInstance(extractor)) {
            return extractorClass.cast(extractor);
        }
        if (nullDefault)
            return null;
        throw new AkibanInternalException(
                "extractor for " + type + " is of class " + extractor.getClass() + ", required " + extractorClass
        );
    }

    private static Map<AkType,? extends LongExtractor> createLongExtractorsMap() {
        Map<AkType,LongExtractor> result = new EnumMap<AkType,LongExtractor>(AkType.class);
        result.put(AkType.DATE, ExtractorsForDates.DATE);
        result.put(AkType.DATETIME, ExtractorsForDates.DATETIME);
        result.put(AkType.INT, ExtractorsForLong.INT);
        result.put(AkType.LONG, ExtractorsForLong.LONG);
        result.put(AkType.TIME, ExtractorsForDates.TIME);
        result.put(AkType.TIMESTAMP, ExtractorsForDates.TIMESTAMP);
        result.put(AkType.U_INT, ExtractorsForLong.U_INT);
        result.put(AkType.YEAR, ExtractorsForDates.YEAR);
        result.put(AkType.INTERVAL_MILLIS, ExtractorsForDates.INTERVAL_MILLIS);
        result.put(AkType.INTERVAL_MONTH, ExtractorsForDates.INTERVAL_MONTH);
        return result;
    }

    private static final BooleanExtractor BOOLEAN_EXTRACTOR = new BooleanExtractor();
    private static final DoubleExtractor DOUBLE_EXTRACTOR = new DoubleExtractor();
    private static final ExtractorForString STRING_EXTRACTOR = new ExtractorForString();
    private static final ExtractorForBigInteger UBIGINT_EXTRACTOR = new ExtractorForBigInteger();
    private static final ExtractorForBigDecimal DECIMAL_EXTRACTOR = new ExtractorForBigDecimal();
    private static final ExtractorForVarBinary VARBINARY_EXTRACTOR = new ExtractorForVarBinary();
    
    private static final Map<AkType,? extends LongExtractor> readOnlyExtractorsMap = createLongExtractorsMap();
}
