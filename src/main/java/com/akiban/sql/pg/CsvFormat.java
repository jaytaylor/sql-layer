/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.pg;

import com.akiban.sql.parser.CopyStatementNode;

import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;

// TODO: Move to another package as part of a separate service.
public class CsvFormat
{
    private String encoding, delimiter, quote, escape, nullString;
    private List<String> headings = null;
    
    private int delimiterByte, quoteByte, escapeByte;
    private byte[] nullBytes, recordEndBytes;

    public CsvFormat(String encoding) {
        this.encoding = encoding;
        this.recordEndBytes = getBytes("\n");
        setDelimiter(",");
        setNullString("");
        setQuote("\"");
    }

    public static CsvFormat create(CopyStatementNode copyStmt, String defaultEncoding) {
        String encoding = copyStmt.getEncoding();
        if (encoding == null)
            encoding = defaultEncoding;
        CsvFormat format = new CsvFormat(encoding);
        if (copyStmt.getDelimiter() != null)
            format.setDelimiter(copyStmt.getDelimiter());
        if (copyStmt.getQuote() != null)
            format.setQuote(copyStmt.getQuote());
        if (copyStmt.getEscape() != null)
            format.setEscape(copyStmt.getEscape());
        if (copyStmt.getNullString() != null)
            format.setNullString(copyStmt.getNullString());
        return format;
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
        byte[] bytes = getBytes(delimiter);
        if (bytes.length != 1)
            throw new IllegalArgumentException("Must encode as a single byte.");
        this.delimiter = delimiter;
        this.delimiterByte = bytes[0];
    }

    public void setQuote(String quote) {
        byte[] bytes = getBytes(quote);
        if (bytes.length != 1)
            throw new IllegalArgumentException("Must encode as a single byte.");
        this.escape = this.quote = quote;
        this.escapeByte = this.quoteByte = bytes[0];
    }

    public void setEscape(String escape) {
        byte[] bytes = getBytes(escape);
        if (bytes.length != 1)
            throw new IllegalArgumentException("Must encode as a single byte.");
        this.escape = escape;
        this.escapeByte = bytes[0];
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
        if (recordEndBytes.length != 1)
            throw new IllegalArgumentException("Must encode as a single byte.");
        return recordEndBytes[0];
    }

    public int getReturn() {
        byte[] bytes = getBytes("\r");
        if (bytes.length != 1)
            throw new IllegalArgumentException("Must encode as a single byte.");
        return bytes[0];
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
    
}
