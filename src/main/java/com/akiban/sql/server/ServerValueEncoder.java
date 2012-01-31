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

package com.akiban.sql.server;

import com.akiban.server.Quote;
import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.error.ZeroDateTimeException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.io.*;

public class ServerValueEncoder
{
    public static enum ZeroDateTimeBehavior {
        NONE(null),
        EXCEPTION("exception"),
        ROUND("round"),
        CONVERT_TO_NULL("convertToNull");

        private String propertyName;

        ZeroDateTimeBehavior(String propertyName) {
            this.propertyName = propertyName;
        }
        
        public static ZeroDateTimeBehavior fromProperty(String name) {
            if (name == null) return NONE;
            for (ZeroDateTimeBehavior zdtb : values()) {
                if (name.equals(zdtb.propertyName))
                    return zdtb;
            }
            throw new IllegalArgumentException(name);
        }
    }

    public static final String ROUND_ZERO_DATETIME = "0001-01-01 00:00:00";
    public static final String ROUND_ZERO_DATE = "0001-01-01";

    private String encoding;
    private ZeroDateTimeBehavior zeroDateTimeBehavior;
    private ByteArrayOutputStream byteStream;
    private PrintWriter printWriter;
    private AkibanAppender appender;
    private FromObjectValueSource objectSource;

    public ServerValueEncoder(String encoding) {
        this.encoding = encoding;

        byteStream = new ByteArrayOutputStream();
        try {
            printWriter = new PrintWriter(new OutputStreamWriter(byteStream, encoding));
        }
        catch (UnsupportedEncodingException ex) {
            throw new UnsupportedCharsetException("", "", encoding);
        }
        // If the target encoding is UTF-8, we can support
        // canAppendBytes() for properly encoded source strings.
        if ("UTF-8".equals(encoding))
            appender = AkibanAppender.of(byteStream, printWriter);
        else
            appender = AkibanAppender.of(printWriter);
    }

    public ServerValueEncoder(String encoding, ZeroDateTimeBehavior zeroDateTimeBehavior) {
        this(encoding);
        this.zeroDateTimeBehavior = zeroDateTimeBehavior;
    }

    public ByteArrayOutputStream getByteStream() {
        printWriter.flush();
        return byteStream;
    }

    public AkibanAppender getAppender() {
        return appender;
    }

    /** Encode the given value into a stream that can then be passed
     * to <code>writeByteStream</code>.
     */
    public ByteArrayOutputStream encodeValue(ValueSource value, ServerType type, 
                                             boolean binary) throws IOException {
        if (value.isNull())
            return null;
        if ((zeroDateTimeBehavior != ZeroDateTimeBehavior.NONE) &&
            (((type.getAkType() == AkType.DATE) &&
              (value.getDate() == 0)) ||
             ((type.getAkType() == AkType.DATETIME) &&
              (value.getDateTime() == 0)))) {
            switch (zeroDateTimeBehavior) {
            case EXCEPTION:
                throw new ZeroDateTimeException();
            case ROUND:
                if (objectSource == null)
                    objectSource = new FromObjectValueSource();
                objectSource.setExplicitly((type.getAkType() == AkType.DATETIME) ?
                                           ROUND_ZERO_DATETIME : ROUND_ZERO_DATE,
                                           AkType.VARCHAR);
                value = objectSource;
                break;
            case CONVERT_TO_NULL:
                return null;
            }
        }
        reset();
        appendValue(value, type, binary);
        return getByteStream();
    }

    /** Encode the given direct value. */
    public ByteArrayOutputStream encodeObject(Object value, ServerType type, 
                                              boolean binary) throws IOException {
        if (value == null)
            return null;
        reset();
        appendObject(value, type, binary);
        return getByteStream();
    }

    /** Reset the contents of the buffer. */
    public void reset() {
        getByteStream().reset();
    }
    
    /** Append the given value to the buffer. */
    public void appendValue(ValueSource value, ServerType type, boolean binary) 
            throws IOException {
        if (type.getAkType() == AkType.VARBINARY) {
            ByteSource bs = Extractors.getByteSourceExtractor().getObject(value);
            byte[] ba = bs.byteArray();
            int offset = bs.byteArrayOffset();
            int length = bs.byteArrayLength();
            if (binary)
                getByteStream().write(ba, offset, length);
            else {
                for (int i = 0; i < length; i++) {
                    printWriter.format("\\%03o", ba[offset+i]);
                }
            }
        }
        else {
            assert !binary;
            value.appendAsString(appender, Quote.NONE);
        }
    }
    
    /** Append the given direct object to the buffer. */
    public void appendObject(Object value, ServerType type, boolean binary) 
            throws IOException {
        AkType akType = type.getAkType();
        if ((akType == AkType.VARCHAR) && (value instanceof String)) {
            // Optimize the common case of directly encoding a string.
            printWriter.write((String)value);
            return;
        }
        if (objectSource == null)
            objectSource = new FromObjectValueSource();
        objectSource.setExplicitly(value, akType);
        appendValue(objectSource, type, binary);
    }

    public void appendString(String string) throws IOException {
        printWriter.write(string);
    }

}
