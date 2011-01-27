package com.akiban.cserver;

import com.akiban.util.AkibanAppender;

import java.util.Formatter;

public enum Quote {
    NONE {
        @Override
        public void append(AkibanAppender sb, String s) {
            sb.append(s);
        }
    },
    SINGLE_QUOTE {
        @Override
        public void append(AkibanAppender sb, String s) {
            doAppend(sb, s, SINGLE_QUOTE_CHAR, false);
        }
    },
    DOUBLE_QUOTE {
        @Override
        public void append(AkibanAppender sb, String s) {
            doAppend(sb, s, DOUBLE_QUOTE_CHAR, false);
        }
    },
    JSON_QUOTE {
        @Override
        public void append(AkibanAppender sb, String s) {
            doAppend(sb, s, DOUBLE_QUOTE_CHAR, true);
        }
    };
    private static final char SINGLE_QUOTE_CHAR = '\'';
    private static final char DOUBLE_QUOTE_CHAR = '"';

    public abstract void append(final AkibanAppender sb, String s);
    
    private static void doAppend(AkibanAppender sb, String s, char quote, boolean escapeControlChars) {
        if (s == null) {
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
