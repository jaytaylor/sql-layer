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

public abstract class LongExtractor extends AbstractExtractor {

    // LongExtractor interface

    public abstract long getLong(ValueSource source);
    public abstract String asString(long value);
    public abstract long getLong(String string);

    // for date/times only
    public abstract long stdLongToUnix (long longVal);
    public abstract long unixToStdLong (long unixVal);
    public abstract long[] getYearMonthDay (long value);
    
    // package-private ctor
    LongExtractor(AkType targetConversionType) {
        super(targetConversionType);
    }
}
