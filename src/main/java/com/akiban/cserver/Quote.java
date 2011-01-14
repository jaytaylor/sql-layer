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
            if (s == null) {
                sb.append(s);
                return;
            }
            sb.append('\'');
            for (int i = 0; i < s.length(); i++) {
                char ch = s.charAt(i);
                if (ch == '\'' || ch == '\\') {
                    sb.append('\\');
                }
                sb.append(ch);
            }
            sb.append('\'');
        }
    },
    DOUBLE_QUOTE {
        @Override
        public void append(StringBuilder sb, String s) {
            if (s == null) {
                sb.append(s);
                return;
            }
            sb.append('\"');
            for (int i = 0; i < s.length(); i++) {
                char ch = s.charAt(i);
                if (ch == '\"' || ch == '\\') {
                    sb.append('\\');
                }
                sb.append(ch);
            }
            sb.append('\"');
        }
    };

    public abstract void append(final StringBuilder sb, String s);
}
