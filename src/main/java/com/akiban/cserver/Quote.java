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

package com.akiban.cserver;

import java.util.Formatter;

public enum Quote {
    NONE {
        @Override
        public void append(StringBuilder sb, String s) {
            sb.append(s);
        }
    },
    SINGLE_QUOTE {
        @Override
        public void append(StringBuilder sb, String s) {
            doAppend(sb, s, SINGLE_QUOTE_CHAR, false);
        }
    },
    DOUBLE_QUOTE {
        @Override
        public void append(StringBuilder sb, String s) {
            doAppend(sb, s, DOUBLE_QUOTE_CHAR, false);
        }
    },
    JSON_QUOTE {
        @Override
        public void append(StringBuilder sb, String s) {
            doAppend(sb, s, DOUBLE_QUOTE_CHAR, true);
        }
    };
    private static final char SINGLE_QUOTE_CHAR = '\'';
    private static final char DOUBLE_QUOTE_CHAR = '"';

    public abstract void append(final StringBuilder sb, String s);
    
    private static void doAppend(StringBuilder sb, String s, char quote, boolean escapeControlChars) {
        if (s == null) {
            sb.append(s);
            return;
        }
        sb.append(quote);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (escapeControlChars && Character.isISOControl(ch)) {
                new Formatter(sb).format("\\u%04x", (int)ch);
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
