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

package com.akiban.server.types.util;

import com.akiban.server.Quote;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.ValueSourceIsNullException;
import com.akiban.server.types.WrongValueGetException;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class AbstractLongValueSource implements ValueSource {

    protected abstract long rawLong();

    // ValueSource interface

    @Override
    public BigDecimal getDecimal() {
        throw complain(AkType.DECIMAL);
    }

    @Override
    public BigInteger getUBigInt() {
        throw complain(AkType.U_BIGINT);
    }

    @Override
    public ByteSource getVarBinary() {
        throw complain(AkType.VARBINARY);
    }

    @Override
    public double getDouble() {
        throw complain(AkType.DOUBLE);
    }

    @Override
    public double getUDouble() {
        throw complain(AkType.U_DOUBLE);
    }

    @Override
    public float getFloat() {
        throw complain(AkType.FLOAT);
    }

    @Override
    public float getUFloat() {
        throw complain(AkType.U_FLOAT);
    }

    @Override
    public long getDate() {
        return longOf(AkType.DATE);
    }

    @Override
    public long getDateTime() {
        return longOf(AkType.DATETIME);
    }

    @Override
    public long getInt() {
        return longOf(AkType.INT);
    }

    @Override
    public long getLong() {
        return longOf(AkType.LONG);
    }

    @Override
    public long getTime() {
        return longOf(AkType.TIME);
    }

    @Override
    public long getTimestamp() {
        return longOf(AkType.TIMESTAMP);
    }

    @Override
    public long getUInt() {
        return longOf(AkType.U_INT);
    }

    @Override
    public long getYear() {
        return longOf(AkType.YEAR);
    }

    @Override
    public String getString() {
        throw complain(AkType.VARCHAR);
    }

    @Override
    public String getText() {
        throw complain(AkType.TEXT);
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        if (isNull()) {
            appender.append(null);
        }
        else {
            LongExtractor extractor = Extractors.getLongExtractor(getConversionType());
            String asString = extractor.asString(rawLong());
            appender.append(asString);
        }
    }

    // Object interface

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('(').append(getConversionType()).append(": ");
        appendAsString(AkibanAppender.of(sb), Quote.NONE);
        sb.append(')');
        return sb.toString();
    }


    // private methods

    private long longOf(AkType expected) {
        ValueSourceHelper.checkType(expected, getConversionType());
        if (isNull()) {
            throw new ValueSourceIsNullException();
        }
        return rawLong();
    }

    private WrongValueGetException complain(AkType expected) {
        return new WrongValueGetException(expected, getConversionType());
    }
}
