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

import com.akiban.server.Quote;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.util.AkibanAppender;

import java.util.EnumMap;
import java.util.Map;

public final class Extractors {
    public static LongExtractor getLongExtractor(AkType type) {
        AbstractExtractor extractor = get(type);
        if (extractor instanceof LongExtractor) {
            return (LongExtractor) extractor;
        }
        return null;
    }

    public static boolean extractBoolean(ValueSource source) {
        return source.getBool();
    }

    private static AbstractExtractor get(AkType type) {
        return readOnlyExtractorsMap.get(type);
    }

    private static Map<AkType,AbstractExtractor> createExtractorsMap() {
        Map<AkType,AbstractExtractor> result = new EnumMap<AkType, AbstractExtractor>(AkType.class);
        result.put(AkType.DATE, ExtractorsForDates.DATE);
        result.put(AkType.DATETIME, ExtractorsForDates.DATETIME);
        result.put(AkType.DECIMAL, null);
        result.put(AkType.DOUBLE, null);
        result.put(AkType.FLOAT, null);
        result.put(AkType.INT, ExtractorsForLong.INT);
        result.put(AkType.LONG, ExtractorsForLong.LONG);
        result.put(AkType.VARCHAR, null);
        result.put(AkType.TEXT, null);
        result.put(AkType.TIME, ExtractorsForDates.TIME);
        result.put(AkType.TIMESTAMP, ExtractorsForDates.TIMESTAMP);
        result.put(AkType.U_BIGINT, null);
        result.put(AkType.U_DOUBLE, null);
        result.put(AkType.U_FLOAT, null);
        result.put(AkType.U_INT, ExtractorsForLong.U_INT);
        result.put(AkType.VARBINARY, null);
        result.put(AkType.YEAR, ExtractorsForDates.YEAR);
        return result;
    }

    private static final Map<AkType,AbstractExtractor> readOnlyExtractorsMap = createExtractorsMap();
}
