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

package com.akiban.sql.pg;

import com.akiban.server.Quote;
import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.io.*;

public class PostgresValueEncoder
{
    private String encoding;
    private ByteArrayOutputStream byteStream;
    private PrintWriter printWriter;
    private AkibanAppender appender;
    private FromObjectValueSource objectSource;

    public PostgresValueEncoder(String encoding) {
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

    public ByteArrayOutputStream getByteStream() {
        printWriter.flush();
        return byteStream;
    }

    /** Encode the given value into a stream that can then be passed
     * to {@link PostgresMessenger#writeByteStream}.
     */
    public ByteArrayOutputStream encodeValue(ValueSource value, PostgresType type, 
                                             boolean binary) throws IOException {
        if (value.isNull())
            return null;
        reset();
        appendValue(value, type, binary);
        return getByteStream();
    }

    /** Encode the given direct value. */
    public ByteArrayOutputStream encodeObject(Object value, PostgresType type, 
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
    public void appendValue(ValueSource value, PostgresType type, boolean binary) 
            throws IOException {
        if (binary) {
            assert (type.getAkType() == AkType.VARBINARY);
            ByteSource asBytes = Extractors.getByteSourceExtractor().getObject(value);
            getByteStream().write(asBytes.byteArray(), asBytes.byteArrayOffset(), asBytes.byteArrayLength());
        }
        else {
            value.appendAsString(appender, Quote.NONE);
        }
    }
    
    /** Append the given direct object to the buffer. */
    public void appendObject(Object value, PostgresType type, boolean binary) 
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
