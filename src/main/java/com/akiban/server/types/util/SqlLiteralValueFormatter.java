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

package com.akiban.server.types.util;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.util.ByteSource;

import com.akiban.server.error.AkibanInternalException;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.io.IOException;
import java.util.Formatter;

public class SqlLiteralValueFormatter
{
    public static String format(ValueSource source) {
        return format(source, source.getConversionType());
    }

    public static String format(ValueSource source, AkType type) {
        StringBuilder str = new StringBuilder();
        SqlLiteralValueFormatter formatter = new SqlLiteralValueFormatter(str);
        try {
            formatter.append(source, type);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("IO error writing to string", ex);
        }
        return str.toString();
    }

    public static enum DateTimeFormat {
        // DATETIME isn't legal SQL, but is suitable for explain.
        // TIMESTAMP does not work for INSERT because of long extractor confusion.
        DATETIME, TIMESTAMP, NONE
    }

    private Appendable buffer;
    private Formatter formatter;
    private DateTimeFormat dateTimeFormat = DateTimeFormat.DATETIME;

    public SqlLiteralValueFormatter(Appendable buffer) {
        this.buffer = buffer;
    }
    
    public DateTimeFormat getDateTimeFormat() {
        return dateTimeFormat;
    }
    public void setDateTimeFormat(DateTimeFormat dateTimeFormat) {
        this.dateTimeFormat = dateTimeFormat;
    }

    public void append(ValueSource source) throws IOException {
        append(source, source.getConversionType());
    }

    public void append(ValueSource source, AkType type) throws IOException {
        if (source.isNull()) {
            buffer.append("NULL");
            return;
        }
        switch (type) {
        case DATE:
            appendDate(source.getDate());
            break;
        case DATETIME:
            appendDateTime(source.getDateTime());
            break;
        case DECIMAL:
            appendDecimal(source.getDecimal());
            break;
        case DOUBLE:
            appendDouble(source.getDouble());
            break;
        case FLOAT:
            appendDouble(source.getFloat());
            break;
        case INT:
            appendLong(source.getInt());
            break;
        case LONG:
            appendLong(source.getLong());
            break;
        case VARCHAR:
            appendVarchar(source.getString());
            break;
        case TEXT:
            appendVarchar(source.getText());
            break;
        case TIME:
            appendTime(source.getTime());
            break;
        case TIMESTAMP:
            appendTimestamp(source.getTimestamp());
            break;
        case U_BIGINT:
            appendUBigInt(source.getUBigInt());
            break;
        case U_DOUBLE:
            appendDouble(source.getUDouble());
            break;
        case U_FLOAT:
            appendDouble(source.getUFloat());
            break;
        case U_INT:
            appendLong(source.getUInt());
            break;
        case VARBINARY:
            appendVarBinary(source.getVarBinary());
            break;
        case YEAR:
            appendLong(source.getYear());
            break;
        case BOOL:
            appendBool(source.getBool());
            break;
        case INTERVAL_MILLIS:
            appendIntervalMillis(source.getInterval_Millis());
            break;
        case INTERVAL_MONTH:
            appendIntervalMonth(source.getInterval_Month());
            break;
        default:
            assert false : type;
            appendVarchar(source.getString());
            break;
        }
    }

    protected void appendDecimal(BigDecimal value) throws IOException {
        buffer.append(value.toString());
    }

    protected void appendDouble(double value) throws IOException {
        if (formatter == null)
            formatter = new Formatter(buffer);
        formatter.format("%e", value);
    }

    protected void appendLong(long value) throws IOException {
        buffer.append(Long.toString(value));
    }

    protected void appendVarchar(String value) throws IOException {
        buffer.append('\'');
        if (value.indexOf('\'') < 0)
            buffer.append(value);
        else {
            for (int i = 0; i < value.length(); i++) {
                int ch = value.charAt(i);
                if (ch == '\'')
                    buffer.append('\'');
                buffer.append((char)ch);
            }
        }
        buffer.append('\'');
    }

    protected void appendUBigInt(BigInteger value) throws IOException {
        buffer.append(value.toString());
    }

    private char hexDigit(int n) {
        if (n < 10)
            return (char)('0' + n);
        else
            return (char)('A' + n - 10);
    }

    protected void appendVarBinary(ByteSource value) throws IOException {
        buffer.append("X'");
        byte[] byteArray = value.byteArray();
        int byteArrayOffset = value.byteArrayOffset();
        int byteArrayLength = value.byteArrayLength();
        for (int i = 0; i < byteArrayLength; i++) {
            int b = byteArray[byteArrayOffset + i] & 0xFF;
            buffer.append(hexDigit(b >> 4));
            buffer.append(hexDigit(b & 0xF));
        }
        buffer.append('\'');
    }

    protected void appendBool(boolean value) throws IOException {
        buffer.append((value) ? "TRUE" : "FALSE");
    }

    LongExtractor dateExtractor, dateTimeExtractor, timeExtractor, timestampExtractor;

    protected void appendDate(long value) throws IOException {
        if (dateExtractor == null)
            dateExtractor = Extractors.getLongExtractor(AkType.DATE);
        buffer.append("DATE '");
        buffer.append(dateExtractor.asString(value));
        buffer.append('\'');
    }

    protected void appendDateTime(long value) throws IOException {
        if (dateExtractor == null)
            dateExtractor = Extractors.getLongExtractor(AkType.DATETIME);
        switch (dateTimeFormat) {
        case DATETIME:
            buffer.append("DATETIME '");
            break;
        case TIMESTAMP:
            buffer.append("TIMESTAMP '");
            break;
        case NONE:
        default:
            buffer.append('\'');
            break;
        }
        buffer.append(dateExtractor.asString(value));
        buffer.append('\'');
    }

    protected void appendTime(long value) throws IOException {
        if (dateExtractor == null)
            dateExtractor = Extractors.getLongExtractor(AkType.TIME);
        buffer.append("TIME '");
        buffer.append(dateExtractor.asString(value));
        buffer.append('\'');
    }

    protected void appendTimestamp(long value) throws IOException {
        if (dateExtractor == null)
            dateExtractor = Extractors.getLongExtractor(AkType.TIMESTAMP);
        buffer.append("TIMESTAMP '");
        buffer.append(dateExtractor.asString(value));
        buffer.append('\'');
    }

    protected void appendIntervalMillis(long value) throws IOException {
        if (formatter == null)
            formatter = new Formatter(buffer);
        buffer.append("INTERVAL '");
        long days, hours, mins, secs, millis;
        if (value < 0) {
            buffer.append('-');
            millis = -value;
        }
        else {
            millis = value;
        }
        // Could be data-driven, but just enough special cases that
        // that would be pretty complicated.
        secs = millis / 1000;
        millis -= secs * 1000;
        mins = secs / 60;
        secs -= mins * 60;
        hours = mins / 60;
        mins -= hours * 60;
        days = hours / 24;
        hours -= days * 24;
        String hi = null, lo = null;
        if (days > 0) {
            formatter.format("%d", days);
            hi = lo = "DAY";
        }
        if ((hours > 0) ||
            ((hi != null) && ((mins > 0) || (secs > 0) || (millis > 0)))) {
            if (hi != null) {
                formatter.format(":%02d", hours);
            }
            else {
                formatter.format("%d", hours);
            }
            lo = "HOUR";
            if (hi == null) hi = lo;
        }
        if ((mins > 0) ||
            ((hi != null) && ((secs > 0) || (millis > 0)))) {
            if (hi != null) {
                formatter.format(":%02d", mins);
            }
            else {
                formatter.format("%d", mins);
            }
            lo = "MINUTE";
            if (hi == null) hi = lo;
        }
        if ((secs > 0) || (hi == null) || (millis > 0)) {
            if (hi != null) {
                formatter.format(":%02d", secs);
            }
            else {
                formatter.format("%d", secs);
            }
            lo = "SECOND";
            if (hi == null) hi = lo;
        }
        if (millis > 0) {
            formatter.format(".%03d", millis);
        }
        buffer.append("' ");
        buffer.append(hi);
        if (hi != lo) {
            buffer.append(" TO ");
            buffer.append(lo);
        }
    }

    protected void appendIntervalMonth(long value) throws IOException {
        if (formatter == null)
            formatter = new Formatter(buffer);
        buffer.append("INTERVAL '");
        long years, months;
        if (value < 0) {
            buffer.append('-');
            months = -value;
        }
        else {
            months = value;
        }
        years = months / 12;
        months -= years * 12;
        String hi = null, lo = null;
        if (years > 0) {
            formatter.format("%d", years);
            hi = lo = "YEAR";
        }
        if ((months > 0) || (hi == null)) {
            if (hi != null) {
                formatter.format("-%02d", months);
            }
            else {
                formatter.format("%d", months);
            }
            lo = "MONTH";
            if (hi == null) hi = lo;
        }
        buffer.append("' ");
        buffer.append(hi);
        if (hi != lo) {
            buffer.append(" TO ");
            buffer.append(lo);
        }
    }
}
