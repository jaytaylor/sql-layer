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

package com.foundationdb.util;

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueTarget;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;

public abstract class AkibanAppender {
    public abstract void append(Object o);
    public abstract void append(char c);
    public abstract void append(long l);
    public abstract void append(String s);
    public abstract Appendable getAppendable();
    public abstract ValueTarget asValueTarget();

    public boolean canAppendBytes() {
        return false;
    }

    public Charset appendBytesAs() {
        throw new UnsupportedOperationException();
    }

    public void appendBytes(byte[] bytes, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    public void appendBytes(ByteSource byteSource) {
        appendBytes(byteSource.byteArray(), byteSource.byteArrayOffset(), byteSource.byteArrayLength());
    }

    public static AkibanAppender of(StringBuilder stringBuilder) {
        return new AkibanAppenderSB(stringBuilder);
    }

    public static AkibanAppender of(PrintWriter printWriter) {
        return new AkibanAppenderPW(printWriter);
    }

    public static AkibanAppender of(OutputStream outputStream, PrintWriter printWriter, String charset) {
        return new AkibanAppenderOS(outputStream, printWriter, charset);
    }

    private static class AkibanAppenderPW extends AbstractAkibanAppender
    {
        private final PrintWriter pr;

        public AkibanAppenderPW(PrintWriter pr) {
            this.pr = pr;
        }

        @Override
        public void append(Object o) {
            pr.print(o);
        }

        @Override
        public void append(char c) {
            pr.print(c);
        }

        @Override
        public void append(long l) {
            pr.print(l);
        }

        @Override
        public void append(String s) {
            pr.print(s);
        }

        @Override
        public Appendable getAppendable() {
            return pr;
        }

        protected void flush() {
            pr.flush();
        }
    }

    private static class AkibanAppenderOS extends AkibanAppenderPW {
        private final OutputStream os;
        private final Charset charset;

        private AkibanAppenderOS(OutputStream os, PrintWriter printWriter, String charset) {
            super(printWriter);
            this.os = os;
            this.charset = Charset.forName(charset);
        }

        @Override
        public void appendBytes(byte[] bytes, int offset, int length) {
            try {
                super.flush();
                os.write(bytes, offset, length);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean canAppendBytes() {
            return true;
        }

        @Override
        public Charset appendBytesAs() {
            return charset;
        }
    }

    private static class AkibanAppenderSB extends AbstractAkibanAppender
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
        public void append(char c) {
            sb.append(c);
        }

        @Override
        public void append(long l) {
            sb.append(l);
        }

        @Override
        public void append(String s) {
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

    private static abstract class AbstractAkibanAppender extends AkibanAppender implements ValueTarget {

        // AkibanAppender interface

        @Override
        public ValueTarget asValueTarget() {
            return this;
        }

        // ValueTarget interface (supported methods)

        @Override
        public AkType getConversionType() {
            return AkType.VARCHAR;
        }

        @Override
        public void putString(String value) {
            append(value);
        }

        @Override
        public void putNull() {
            append(null);
        }

        // ValueTarget interface (unsupported methods)

        @Override
        public void putDate(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDateTime(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDecimal(BigDecimal value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putDouble(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putFloat(float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInt(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putLong(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putText(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTime(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTimestamp(long value) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void putInterval_Millis(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putInterval_Month(long value) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void putUBigInt(BigInteger value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putUDouble(double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putUFloat(float value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putUInt(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putVarBinary(ByteSource value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putYear(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putBool(boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putResultSet(Cursor value) {
            throw new UnsupportedOperationException();
        }
    }
}
