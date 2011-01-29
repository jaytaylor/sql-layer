package com.akiban.util;

import java.io.PrintWriter;

public abstract class AkibanAppender {
    public abstract void append(Object o);
    public abstract void write(char c);
    public abstract void write(String s);
    public abstract Appendable getAppendable();

    public static AkibanAppender of(StringBuilder stringBuilder) {
        return new AkibanAppenderSB(stringBuilder);
    }

    public static AkibanAppender of(PrintWriter printWriter) {
        return new AkibanAppenderPW(printWriter);
    }

    private static class AkibanAppenderPW extends AkibanAppender
    {
        private final PrintWriter pr;

        public AkibanAppenderPW(PrintWriter pr) {
            this.pr = pr;
        }

        @Override
        public void append(Object o) {
            pr.append(o == null ? "null" : o.toString());
        }

        @Override
        public void write(char c) {
            pr.print(c);
        }

        @Override
        public void write(String s) {
            pr.print(s);
        }

        @Override
        public Appendable getAppendable() {
            return pr;
        }
    }

    private static class AkibanAppenderSB extends AkibanAppender
    {
        private final StringBuilder sb;

        public AkibanAppenderSB(StringBuilder sb) {
            this.sb = sb;
        }

        @Override
        public void append(Object o) {
            sb.append(o);
        }

        @Override
        public void write(char c) {
            sb.append(c);
        }

        @Override
        public void write(String s) {
            sb.append(s);
        }

        @Override
        public Appendable getAppendable() {
            return sb;
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
