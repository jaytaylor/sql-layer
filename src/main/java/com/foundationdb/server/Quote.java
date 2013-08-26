/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server;

import com.foundationdb.server.types.AkType;
import com.foundationdb.util.AkibanAppender;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;

import static com.foundationdb.server.types.AkType.*;

public enum Quote {
    NONE(null, false),
    SINGLE_QUOTE('\'', false),
    DOUBLE_QUOTE('"', false),
    JSON_QUOTE('"', true,
            DATE, DATETIME, DECIMAL, VARCHAR, TEXT, TIME, TIMESTAMP, VARBINARY, YEAR)
    ;

    private final Character quoteChar;
    private final boolean escapeControlChars;
    private final EnumSet<AkType> quotedTypes;

    Quote(Character quoteChar, boolean escapeControlChars, AkType... quotedTypes) {
        this.quoteChar = quoteChar;
        this.escapeControlChars = escapeControlChars;
        this.quotedTypes = EnumSet.noneOf(AkType.class);
        Collections.addAll(this.quotedTypes, quotedTypes);
    }

    private final static Charset ASCII = Charset.forName("US-ASCII");
    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final static Charset LATIN1 = charset("LATIN1");

    private static Charset charset(String name) {
        try {
            return Charset.forName(name);
        } catch (Exception e) {
            return null;
        }
    }

    @Deprecated
    public void quote(AkibanAppender appender, AkType type) {
        if ((quoteChar != null) && (!quotedTypes.isEmpty()) && quotedTypes.contains(type)) {
            appender.append(quoteChar.charValue());
        }
    }

    public void append(AkibanAppender sb, String s) {
        doAppend(sb, s, quoteChar, escapeControlChars);
    }

    public void append(AkibanAppender appender, ByteBuffer byteBuffer, String charset) {
        append(appender, byteBuffer, Charset.forName(charset));
    }

    public void append(AkibanAppender appender, ByteBuffer byteBuffer, Charset charset) {
        if (!byteBuffer.hasArray()) {
            throw new IllegalArgumentException("ByteBuffer needs backing array");
        }
        byte[] bytes = byteBuffer.array();
        int offset = byteBuffer.position();
        int length = byteBuffer.limit() - offset;
        
        if ( (!appender.canAppendBytes()) || !writeBytesCharset(charset))
        {
            String string = new String(bytes, offset, length, charset);
            doAppend(appender, string, quoteChar, escapeControlChars);
            return;
        }
        writeBytes(appender, bytes, offset, length, charset, this);
    }

    static void writeBytes(AkibanAppender appender, byte[] bytes, int offset, int length, Charset charset, Quote quote)
    {
        if (! writeBytesCharset(charset) ) {
            throw new IllegalArgumentException(charset.name());
        }
        int wrote = writeDirect(appender, bytes, offset, length, charset, quote.escapeControlChars);
        assert !(wrote > length) : "wrote " + wrote + " of " + length;
        if (wrote < length) {
            String string = new String(bytes, offset + wrote, length - wrote, charset);
            doAppend(appender, string, quote.quoteChar, quote.escapeControlChars);
        }
    }

    private static boolean writeBytesCharset(Charset charset) {
        return      ASCII.equals(charset)
                ||  UTF8.equals(charset)
                || (LATIN1 != null && LATIN1.equals(charset) )
                ;
    }

    private static boolean identityByte(byte b, Charset charset) {
        if (ASCII.equals(charset) || UTF8.equals(charset)) {
            return b >= 0;
        }
        if (LATIN1 != null && LATIN1.equals(charset)) {
            return b >= 0x20 && b <= 0x7E;
        }
        throw new IllegalArgumentException(charset == null ? "null" : charset.name());
    }

    private static int writeDirect(AkibanAppender appender, byte[] bytes, int offset, int length, Charset charset,
                                   boolean needsEscaping) {
        int pos = 0;
        while ( (pos < length)
                && identityByte(bytes[offset+pos], charset)
                && !(needsEscaping && needsEscaping((char)bytes[offset+pos]))
        ) {
            ++pos;
        }
        if (pos > 0) {
            appender.appendBytes(bytes, offset, pos);
        }
        return pos;
    }

    private static boolean needsEscaping(char ch) {
        // Anything other than printing ASCII.
        return (ch >= 0200) || Character.isISOControl(ch);
    }

    private static final String SIMPLY_ESCAPED = "\r\n\t";
    private static final String SIMPLY_ESCAPES = "rnt";

    static void doAppend(AkibanAppender sb, String s, Character quote, boolean escapeControlChars) {
        if (s == null) {
            sb.append(null);
            return;
        }
        if (quote == null) {
            if (escapeControlChars) {
                // this is not put in as an assert, so that we can unit test it
                throw new AssertionError("can't escape without quoting, as a simplification to the code");
            }
            sb.append(s);
            return;
        }

        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (escapeControlChars && needsEscaping(ch)) {
                int idx = SIMPLY_ESCAPED.indexOf(ch);
                if (idx < 0) {
                    new Formatter(sb.getAppendable()).format("\\u%04x", (int)ch);
                }
                else {
                    sb.append('\\');
                    sb.append(SIMPLY_ESCAPES.charAt(idx));
                }
            }
            else {
                if (ch == quote || ch == '\\') {
                    sb.append('\\');
                }
                sb.append(ch);
            }
        }
    }
}
