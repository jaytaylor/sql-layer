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

import com.akiban.util.AkibanAppender;

public abstract class ConversionSourceAppendHelper {

    // for use by subclasses

    protected abstract AkType akType();

    protected void appendNull(AkibanAppender appender) {
        appender.append("null");
    }

    protected void appendDate(AkibanAppender appender) {
        appender.append(source().getDate());
    }

    protected void appendDateTime(AkibanAppender appender) {
        appender.append(source().getDateTime());
    }

    protected void appendDecimal(AkibanAppender appender) {
        appender.append(source().getDecimal());
    }

    protected void appendDouble(AkibanAppender appender) {
        appender.append(source().getDouble());
    }

    protected void appendFloat(AkibanAppender appender) {
        appender.append(source().getFloat());
    }

    protected void appendInt(AkibanAppender appender) {
        appender.append(source().getInt());
    }

    protected void appendLong(AkibanAppender appender) {
        appender.append(source().getLong());
    }
    protected void appendString(AkibanAppender appender) {
        appender.append(source().getString());
    }

    protected void appendText(AkibanAppender appender) {
        appender.append(source().getText());
    }

    protected void appendTime(AkibanAppender appender) {
        appender.append(source().getTime());
    }

    protected void appendTimeStamp(AkibanAppender appender) {
        appender.append(source().getTimestamp());
    }

    protected void appendUBigInt(AkibanAppender appender) {
        appender.append(source().getUBigInt());
    }
    
    protected void appendUDouble(AkibanAppender appender) {
        appender.append(source().getUDouble());
    }
    
    protected void appendUFloat(AkibanAppender appender) {
        appender.append(source().getUFloat());
    }

    protected void appendUInt(AkibanAppender appender) {
        appender.append(source().getUInt());
    }

    protected void appendVarBinary(AkibanAppender appender) {
        appender.append(source().getVarBinary());
    }

    protected void appendYear(AkibanAppender appender) {
        appender.append(source().getYear());
    }

    protected final ConversionSource source() {
        if (source == null) {
            throw new IllegalStateException("source has not been set");
        }
        return source;
    }

    // public interface

    /**
     * <p>Writes to the given appender, using the appropriate appendFoo method based on {@linkplain #akType()}. For
     * instance, if the type is of {@link AkType#LONG},this will delegate to {@link #appendLong(AkibanAppender)}.</p>
     * <p>By default, each of those methods simply gets the appropriate value from the set ConversionSource and
     * appends it to the appender. If you want more efficient behavior for a given method, just override it.</p>
     * @param appender the output appender
     * @throws NullPointerException if {@linkplain #akType()} returns a null
     * @throws UnsupportedOperationException if {@linkplain #akType()} returns an unsupported type, which should
     * only happen with {@link AkType#UNSUPPORTED}
     * @throws IllegalStateException if a ConversionSource hasn't been set
     */
    public void appendTo(AkibanAppender appender) {
        AkType type = akType();
        switch (type) {
            case NULL: appendNull(appender); break;
            case DATE: appendDate(appender); break;
            case DATETIME: appendDateTime(appender); break;
            case DECIMAL: appendDecimal(appender); break;
            case DOUBLE: appendDouble(appender); break;
            case FLOAT: appendFloat(appender); break;
            case INT: appendInt(appender); break;
            case LONG: appendLong(appender); break;
            case STRING: appendString(appender); break;
            case TEXT: appendText(appender); break;
            case TIME: appendTime(appender); break;
            case TIMESTAMP: appendTimeStamp(appender); break;
            case U_BIGINT: appendUBigInt(appender); break;
            case U_DOUBLE: appendUDouble(appender); break;
            case U_FLOAT: appendUFloat(appender); break;
            case U_INT: appendUInt(appender); break;
            case VARBINARY: appendVarBinary(appender); break;
            case YEAR: appendYear(appender); break;
            default: throw new UnsupportedOperationException(type.name());
        }
    }

    /**
     * Sets the target ConversionSource. All subsequent appends will query this source for their values. Pass in
     * a null to un-set.
     * @param source the new source to get values from
     * @return this instance; provided for easy chaining
     */
    public ConversionSourceAppendHelper source(ConversionSource source) {
        this.source = source;
        return this;
    }

    // object state

    private ConversionSource source;
}
