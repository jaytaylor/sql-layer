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
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;

abstract class LongConverter extends AbstractConverter {

    // defined in subclasses

    protected abstract void putLong(ValueTarget target, long value);

    @Override
    protected AkType targetConversionType() {
        return extractor().targetConversionType();
    }

    @Override
    protected final void doConvert(ValueSource source, ValueTarget target) {
        putLong(target, extractor().getLong(source));
    }

    protected final LongExtractor extractor() {
        return extractor;
    }

    protected LongConverter(AkType type) {
        extractor = Extractors.getLongExtractor(type);
    }

    private final LongExtractor extractor;

    // consts


    static final LongConverter LONG = new LongConverter(AkType.LONG) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putLong(value);
        }
    };

    static final LongConverter INT = new LongConverter(AkType.INT) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putInt(value);
        }
    };

    static final LongConverter U_INT = new LongConverter(AkType.U_INT) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putUInt(value);
        }
    };

    static final LongConverter DATE = new LongConverter(AkType.DATE) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putDate(value);
        }
    };

    static final LongConverter DATETIME = new LongConverter(AkType.DATETIME) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putDateTime(value);
        }
    };

    static final LongConverter TIME = new LongConverter(AkType.TIME) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putTime(value);
        }
    };

    static final LongConverter TIMESTAMP = new LongConverter(AkType.TIMESTAMP) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putTimestamp(value);
        }
    };

    static final LongConverter INTERVAL_MILLIS = new LongConverter(AkType.INTERVAL_MILLIS)
    {
        @Override
        protected void putLong(ValueTarget target, long value)
        {
            target.putInterval_Millis(value);
        }
    };
    
    static final LongConverter YEAR = new LongConverter(AkType.YEAR) {
        @Override
        protected void putLong(ValueTarget target, long value) {
            target.putYear(value);
        }
    };
}
