/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types.extract;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.joda.time.DateTimeZone;

public abstract class LongExtractor extends AbstractExtractor {

    // LongExtractor interface

    public abstract long getLong(ValueSource source);
    public abstract String asString(long value);
    public abstract long getLong(String string);

    // for date/times only
    public abstract long stdLongToUnix (long longVal);
    public abstract long unixToStdLong (long unixVal);
    public abstract long[] getYearMonthDayHourMinuteSecond (long value);
    public abstract long getEncoded (long ymd_hms[]);
    
    public abstract long stdLongToUnix (long longVal, DateTimeZone tz);
    public abstract long unixToStdLong (long unixVal, DateTimeZone tz);
    public abstract long[] getYearMonthDayHourMinuteSecond (long value, DateTimeZone tz);
    public abstract long getEncoded (long ymd_hms[], DateTimeZone tz);
    
    // package-private ctor
    LongExtractor(AkType targetConversionType) {
        super(targetConversionType);
    }
}
