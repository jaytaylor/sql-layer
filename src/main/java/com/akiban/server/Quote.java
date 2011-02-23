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

package com.akiban.server;

import com.akiban.util.AkibanAppender;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Formatter;

public enum Quote {
    NONE(null, false),
    SINGLE_QUOTE('\'', false),
    DOUBLE_QUOTE('"', false),
    JSON_QUOTE('"', true)
    ;

    private final Character quoteChar;
    private final boolean escapeControlChars;

    Quote(Character quoteChar, boolean escapeControlChars) {
        this.quoteChar = quoteChar;
        this.escapeControlChars = escapeControlChars;
    }

    private final static Charset ASCII = Charset.forName("US-ASCII");
    private final static Charset UTF8 = Charset.forName("UTF-8");

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
        
        if ( (!appender.canAppendBytes()) || !(ASCII.equals(charset) || UTF8.equals(charset)))
        {
            String string = new String(bytes, offset, length, charset);
            doAppend(appender, string, quoteChar, escapeControlChars);
            return;
        }
        writeBytes(appender, bytes, offset, length, charset);
    }

    static void writeBytes(AkibanAppender appender, byte[] bytes, int offset, int length, Charset charset)
    {
        if (! (ASCII.equals(charset) || UTF8.equals(charset)) ) {
            throw new IllegalArgumentException(charset.name());
        }
        int wrote = writeDirect(appender, bytes, offset, length);
        assert !(wrote > length) : "wrote " + wrote + " of " + length;
        if (wrote < length) {
            String string = new String(bytes, offset + wrote, length - wrote, charset);
            appender.append(string);
        }
    }

    private static int writeDirect(AkibanAppender appender, byte[] bytes, int offset, int length) {
        int pos = 0;
        while ( (pos < length) && (bytes[offset+pos] >= 0)) {
            ++pos;
        }
        if (pos > 0) {
            appender.appendBytes(bytes, offset, pos);
        }
        return pos;
    }


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

        sb.append(quote);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (escapeControlChars && Character.isISOControl(ch)) {
                new Formatter(sb.getAppendable()).format("\\u%04x", (int)ch);
            }
            else {
                if (ch == quote || ch == '\\') {
                    sb.append('\\');
                }
                sb.append(ch);
            }
        }
        sb.append(quote);
    }
}
