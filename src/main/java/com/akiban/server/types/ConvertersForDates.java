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

abstract class ConvertersForDates extends LongConverter {

    final static LongConverter DATE = new ConvertersForDates(AkType.DATE) {
        @Override protected long doGetLong(ConversionSource source)             { return source.getDate(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putDate(value); }
    };

    final static LongConverter DATETIME = new ConvertersForDates(AkType.DATETIME) {
        @Override protected long doGetLong(ConversionSource source)             { return source.getDateTime(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putDateTime(value); }
    };

    final static LongConverter TIME = new ConvertersForDates(AkType.TIME) {
        @Override protected long doGetLong(ConversionSource source)             { return source.getTime(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putTime(value); }
    };

    final static LongConverter TIMESTAMP = new ConvertersForDates(AkType.TIMESTAMP) {
        @Override protected long doGetLong(ConversionSource source)             { return source.getTimestamp(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putTimestamp(value); }
    };

    final static LongConverter YEAR = new ConvertersForDates(AkType.YEAR) {
        @Override protected long doGetLong(ConversionSource source)             { return source.getYear(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putYear(value); }
    };

    protected abstract long doGetLong(ConversionSource source);

    @Override
    public long getLong(ConversionSource source) {
        AkType type = source.getConversionType();
        if (type != requiredType) {
            throw unsupportedConversion(type);
        }
        return doGetLong(source);
    }

    private ConvertersForDates(AkType requiredType) {
        this.requiredType = requiredType;
    }

    private final AkType requiredType;
}
