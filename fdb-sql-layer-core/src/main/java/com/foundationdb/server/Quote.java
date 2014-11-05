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

import com.foundationdb.util.AkibanAppender;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Formatter;

public enum Quote {
    NONE(null, false),
    SINGLE_QUOTE('\'', false),
    DOUBLE_QUOTE('"', false),
    JSON_QUOTE('"', true);

    private final Character quoteChar;
    private final boolean escapeControlChars;

    Quote(Character quoteChar, boolean escapeControlChars) {
        this.quoteChar = quoteChar;
        this.escapeControlChars = escapeControlChars;
    }

    public void append(AkibanAppender sb, String s) {
        doAppend(sb, s, quoteChar, escapeControlChars);
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
