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

package com.foundationdb.server.service.externaldata;

import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;

public class CsvFormat
{
    private String encoding, delimiter, quote, escape, nullString;
    private List<String> headings = null;
    
    private int delimiterByte, quoteByte, escapeByte;
    private byte[] nullBytes, recordEndBytes, requiresQuoting;

    public CsvFormat(String encoding) {
        this.encoding = encoding;
        this.recordEndBytes = getBytes("\n");
        setDelimiter(",");
        setNullString("");
        setQuote("\"");
    }

    public String getEncoding() {
        return encoding;
    }
    public String getDelimiter() {
        return delimiter;
    }
    public String getQuote() {
        return quote;
    }
    public String getEscape() {
        return escape;
    }
    public String getNullString() {
        return nullString;
    }

    public int getDelimiterByte() {
        return delimiterByte;
    }
    public int getQuoteByte() {
        return quoteByte;
    }
    public int getEscapeByte() {
        return escapeByte;
    }
    public byte[] getNullBytes() {
        return nullBytes;
    }

    public List<String> getHeadings() {
        return headings;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        this.delimiterByte = getSingleByte(delimiter);
    }

    public void setQuote(String quote) {
        this.escape = this.quote = quote;
        this.escapeByte = this.quoteByte = getSingleByte(quote);
    }

    public void setEscape(String escape) {
        this.escape = escape;
        this.escapeByte = getSingleByte(escape);
    }

    public void setNullString(String nullString) {
        this.nullString = nullString;
        this.nullBytes = getBytes(nullString);
    }

    public void setHeadings(List<String> headings) {
        this.headings = headings;
    }

    public byte[] getHeadingBytes(int i) {
        return getBytes(headings.get(i));
    }

    public byte[] getRecordEndBytes() {
        return recordEndBytes;
    }

    public int getNewline() {
        return getSingleByte(recordEndBytes);
    }

    public int getReturn() {
        return getSingleByte("\r");
    }

    public byte[] getRequiresQuoting() {
        if (requiresQuoting == null) {
            requiresQuoting = new byte[(quoteByte == escapeByte) ? 4 : 5];
            requiresQuoting[0] = (byte)delimiterByte;
            requiresQuoting[1] = (byte)quoteByte;
            requiresQuoting[2] = (byte)getNewline();
            requiresQuoting[3] = (byte)getReturn();
            if (quoteByte != escapeByte)
                requiresQuoting[4] = (byte)escapeByte;
        }
        return requiresQuoting;
    }

    private byte[] getBytes(String str) {
        try {
            return str.getBytes(encoding);
        }
        catch (UnsupportedEncodingException ex) {
            UnsupportedCharsetException nex = new UnsupportedCharsetException(encoding);
            nex.initCause(ex);
            throw nex;
        }
    }
    
    private int getSingleByte(String str) {
        return getSingleByte(getBytes(str));
    }

    private int getSingleByte(byte[] bytes) {
        if (bytes.length != 1)
            throw new IllegalArgumentException("Must encode as a single byte.");
        return bytes[0] & 0xFF;
    }

}
