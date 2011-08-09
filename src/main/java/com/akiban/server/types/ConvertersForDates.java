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

    final static LongConverter DATE = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getDate(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putDate(value); }
        @Override protected AkType requiredType() { return AkType.DATE; }
    };

    final static LongConverter DATETIME = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getDateTime(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putDateTime(value); }
        @Override protected AkType requiredType() { return AkType.DATETIME; }
    };

    final static LongConverter TIME = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getTime(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putTime(value); }
        @Override protected AkType requiredType() { return AkType.TIME; }
    };

    final static LongConverter TIMESTAMP = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getTimestamp(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putTimestamp(value); }
        @Override protected AkType requiredType() { return AkType.TIMESTAMP; }
    };

    final static LongConverter YEAR = new ConvertersForDates() {
        @Override protected long doGetLong(ConversionSource source)             { return source.getYear(); }
        @Override protected void putLong(ConversionTarget target, long value)   { target.putYear(value); }
        @Override protected AkType requiredType() { return AkType.YEAR; }
    };

    protected abstract long doGetLong(ConversionSource source);

    @Override
    public long getLong(ConversionSource source) {
        AkType type = source.getConversionType();
        if (type != requiredType()) {
            throw unsupportedConversion(type);
        }
        return doGetLong(source);
    }

    protected abstract AkType requiredType();

    private ConvertersForDates() {}
}
