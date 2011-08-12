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
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class FromObjectConversionSource implements ConversionSource {

    // FromObjectConversionSource interface

    public FromObjectConversionSource setExplicitly(Object object, AkType type) {
        setReflectively(object);
        if (akType != AkType.NULL) {
            akType = type;
        }
        return this;
    }

    public FromObjectConversionSource setReflectively(Object object) {
        if (object == null) {
            setNull();
            return this;
        }
        
        final AkType asType;

        if (object instanceof Integer || object instanceof Long)
            asType = AkType.LONG;
        else if (object instanceof String)
            asType = AkType.VARCHAR;
        else if (object instanceof Double)
            asType = AkType.DOUBLE;
        else if (object instanceof Float)
            asType = AkType.FLOAT;
        else if (object instanceof BigDecimal)
            asType = AkType.DECIMAL;
        else if (object instanceof ByteSource || object instanceof byte[])
            asType = AkType.VARBINARY;
        else if (object instanceof BigInteger)
             asType = AkType.U_BIGINT;
        else if (object instanceof Character) {
            object = String.valueOf(object);
            asType = AkType.VARCHAR;
        }
        else throw new UnsupportedOperationException("can't reflectively set " + object.getClass() + ": " + object);

        set(object, asType);
        return this;
    }

    public void setNull() {
        this.akType = AkType.NULL;
        this.object = null;
    }

    // ConversionSource interface

    @Override
    public boolean isNull() {
        return AkType.NULL.equals(akType);
    }

    @Override
    public BigDecimal getDecimal() {
        return as(BigDecimal.class, AkType.DECIMAL);
    }

    @Override
    public BigInteger getUBigInt() {
        return as(BigInteger.class, AkType.U_BIGINT);
    }

    @Override
    public ByteSource getVarBinary() {
        return as(ByteSource.class, AkType.VARBINARY);
    }

    @Override
    public double getDouble() {
        return as(Double.class, AkType.DOUBLE);
    }

    @Override
    public double getUDouble() {
        return as(Double.class, AkType.U_DOUBLE);
    }

    @Override
    public float getFloat() {
        return as(Float.class, AkType.FLOAT);
    }

    @Override
    public float getUFloat() {
        return as(Float.class, AkType.U_FLOAT);
    }

    @Override
    public long getDate() {
        return as(Long.class, AkType.DATE);
    }

    @Override
    public long getDateTime() {
        return as(Long.class, AkType.DATETIME);
    }

    @Override
    public long getInt() {
        return as(Long.class, AkType.INT);
    }

    @Override
    public long getLong() {
        return as(Long.class, AkType.LONG);
    }

    @Override
    public long getTime() {
        return as(Long.class, AkType.TIME);
    }

    @Override
    public long getTimestamp() {
        return as(Long.class, AkType.TIMESTAMP);
    }

    @Override
    public long getUInt() {
        return as(Long.class, AkType.U_INT);
    }

    @Override
    public long getYear() {
        return as(Long.class, AkType.YEAR);
    }

    @Override
    public String getString() {
        return as(String.class, AkType.VARCHAR);
    }

    @Override
    public String getText() {
        return as(String.class, AkType.TEXT);
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        AkType type = getConversionType();
        quote.quote(appender, type);
        if (type == AkType.UNSUPPORTED) {
            throw new IllegalStateException("source object not set");
        }
        quote.append(appender, String.valueOf(object));
        quote.quote(appender, type);
    }

    @Override
    public AkType getConversionType() {
        return akType;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("ConversionSource(%s %s)", akType, object);
    }

    // private methods

    private <T> T as(Class<T> castClass, AkType type) {
        ConversionHelper.checkType(akType, type);
        try {
            return castClass.cast(object);
        } catch (ClassCastException e) {
            String className = object == null ? "null" : object.getClass().getName();
            throw new ClassCastException("casting " + className + " to " + castClass);
        }
    }

    private void set(Object object, AkType asType) {
        if (asType.equals(AkType.UNSUPPORTED)) {
            throw new IllegalArgumentException("can't set to UNSUPPORTED");
        }
        if (object == null) {
            setNull();
        }
        else {
            this.akType = asType;
            this.object = LegacyTransformations.TRIVIAL_TRANSFORMATIONS.tryTransformations(asType, object);
        }
    }

    // object state

    private Object object;
    private AkType akType = AkType.UNSUPPORTED;
}
