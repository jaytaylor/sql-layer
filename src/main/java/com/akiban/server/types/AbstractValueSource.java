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

import com.akiban.server.Quote;
import com.akiban.server.types.extract.Extractors;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class AbstractValueSource implements ValueSource {

    // ValueSource interface

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        appender.append(Extractors.getStringExtractor().getObject(this));
    }
    
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
        throw complain(AkType.DATE);
    }

    @Override
    public long getDateTime() {
        throw complain(AkType.DATETIME);
    }

    @Override
    public long getInt() {
        throw complain(AkType.INT);
    }

    @Override
    public long getLong() {
        throw complain(AkType.LONG);
    }

    @Override
    public long getTime() {
        throw complain(AkType.TIME);
    }

    @Override
    public long getTimestamp() {
        throw complain(AkType.TIMESTAMP);
    }

    @Override
    public long getInterval_Millis()
    {
        throw complain(AkType.INTERVAL_MILLIS);
    }

    @Override
    public long getUInt() {
        throw complain(AkType.U_INT);
    }

    @Override
    public long getYear() {
        throw complain(AkType.YEAR);
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
    public boolean getBool() {
        throw complain(AkType.BOOL);
    }
    
    // for use in this class
    RuntimeException complain(AkType requiredType) {
        AkType actualType = getConversionType();
        return (actualType == requiredType)
                ? new UnsupportedOperationException()
                : new WrongValueGetException(requiredType, actualType);
    }
}
