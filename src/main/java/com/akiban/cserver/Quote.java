package com.akiban.cserver;

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
            doAppend(sb, s, '\'');
        }
    },
    DOUBLE_QUOTE {
        @Override
        public void append(StringBuilder sb, String s) {
            doAppend(sb, s, '"');
        }
    };

    public abstract void append(final StringBuilder sb, String s);
    
    private static void doAppend(StringBuilder sb, String s, char quote) {
        if (s == null) {
            sb.append(s);
            return;
        }
        sb.append(quote);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == quote || ch == '\\') {
                sb.append('\\');
            }
            sb.append(ch);
        }
        sb.append(quote);
    }
}
