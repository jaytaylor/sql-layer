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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.api.dml.scan.NewRow;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;

/** Read from a mysqldump -t file of INSERT statements (with
 * relatively little flexibility).
 */
public class MysqlDumpRowReader extends RowReader
{
    private final byte[] insert, into, values, lock, unlock;
    private enum State { 
        STATEMENT_START, SINGLE_LINE_COMMENT, DELIMITED_COMMENT,
        STATEMENT_VERB, IGNORED_STATEMENT, IGNORED_BACKQUOTE,
        INSERT, INSERT_TABLE, TABLE_BACKQUOTE, INSERT_VALUES,
        NEXT_ROW_CTOR, AFTER_ROW_CTOR,
        NEXT_FIELD, UNQUOTED_FIELD, QUOTED_FIELD, AFTER_QUOTED_FIELD
    };
    private State state;
    private byte[] tableName = null; // According to the file.
    
    public MysqlDumpRowReader(UserTable table, List<Column> columns, String encoding,
                              QueryContext queryContext) {
        super(table, columns, encoding, getBytes("NULL", encoding), queryContext);
        this.insert = getBytes("INSERT", encoding);
        this.into = getBytes("INTO", encoding);
        this.values = getBytes("VALUES", encoding);
        this.lock = getBytes("LOCK", encoding);
        this.unlock = getBytes("UNLOCK", encoding);
        this.state = State.STATEMENT_START;
    }

    private static byte[] getBytes(String str, String encoding) {
        try {
            return str.getBytes(encoding);
        }
        catch (UnsupportedEncodingException ex) {
            UnsupportedCharsetException nex = new UnsupportedCharsetException(encoding);
            nex.initCause(ex);
            throw nex;
        }
    }

    @Override
    public NewRow nextRow(InputStream inputStream) throws IOException {
        int lb = -1;
        while (true) {
            int b = lb;
            if (b < 0) {
                b = inputStream.read();
            }
            else {
                lb = -1;
            }
            switch (state) {
            case STATEMENT_START:
                if (b < 0) {
                    return null;
                }
                else if (b == '-') {
                    b = inputStream.read();
                    if (b == '-') {
                        state = State.SINGLE_LINE_COMMENT;
                    }
                    else {
                        throw new IOException("Unexpected token " +
                                              new String(new byte[] { 
                                                             (byte)'-', (byte)b 
                                                         },
                                                         encoding));
                    }
                }
                else if (b == '/') {
                    b = inputStream.read();
                    if (b == '*') {
                        state = State.DELIMITED_COMMENT;
                    }
                    else {
                        throw new IOException("Unexpected token " +
                                              new String(new byte[] { 
                                                             (byte)'/', (byte)b 
                                                         },
                                                         encoding));
                    }
                }
                else if ((b >= 'A') && (b <= 'Z')) {
                    addToField(b);
                    state = State.STATEMENT_VERB;
                }
                else if ((b == ' ') || (b == '\r') || (b == '\n')) {
                }
                else {
                    throw new IOException("Unexpected token " + (char)b);
                }
                break;
            case SINGLE_LINE_COMMENT:
                if (b < 0) {
                    return null;
                }
                else if (b == '\n') {
                    state = State.STATEMENT_START;
                }
                break;
            case DELIMITED_COMMENT:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a comment");
                }
                else if (b == '*') {
                    lb = inputStream.read();
                    if (lb == '/') {
                        lb = inputStream.read();
                        if (lb == ';') 
                            lb = -1; // Allow stray ; after comment.
                        state = State.STATEMENT_START;
                    }
                }
                break;
            case STATEMENT_VERB:
            case INSERT:
            case INSERT_VALUES:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if ((b >= 'A') && (b <= 'Z')) {
                    addToField(b);
                }
                else {
                    if (b != ' ') lb = b;
                    if (state == State.INSERT) {
                        if (fieldMatches(into)) {
                            state = State.INSERT_TABLE;
                        }
                        else {
                            throw new IOException("Unrecognized statement INSERT " +
                                                  decodeField());
                        }
                    }
                    else if (state == State.INSERT_VALUES) {
                        if (fieldMatches(values)) {
                            state = State.NEXT_ROW_CTOR;
                        }
                        else {
                            throw new IOException("Unrecognized statement INSERT INTO " +
                                                  decodeField());
                        }
                    }
                    else if (fieldMatches(lock) || fieldMatches(unlock)) {
                        state = State.IGNORED_STATEMENT;
                    }
                    else {
                        throw new IOException("Unrecognized statement " + decodeField());
                    }
                }
                break;
            case IGNORED_STATEMENT:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == ';') {
                    state = State.STATEMENT_START;
                }
                else if (b == '`') {
                    state = State.IGNORED_BACKQUOTE;
                }
                break;
            case IGNORED_BACKQUOTE:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == '`') {
                    state = State.IGNORED_STATEMENT;
                }
                else if (b == '\\') {
                    b = inputStream.read();
                }
                break;
            case INSERT_TABLE:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == '`') {
                    addToField(b);
                    state = State.TABLE_BACKQUOTE;
                }
                else if ((b == '.') || 
                         ((b >= 'A') && (b <= 'Z')) ||
                         ((b >= 'a') && (b <= 'z')) ||
                         ((b >= '0') && (b <= '9')) ||
                         (b == '_')) {
                    // Unquoted or qualified table name.
                    addToField(b);
                }
                else {
                    if (b != ' ') lb = b;
                    if (tableName == null) {
                        tableName = copyField();
                    }
                    else if (!fieldMatches(tableName)) {
                        throw new IOException("INSERT INTO changed from " + 
                                              new String(tableName, encoding) +
                                              " to " + decodeField() +
                                              ". Does file contain multiple tables?");
                    }
                }
                break;
            case TABLE_BACKQUOTE:
                if (b < 0) {
                    throw new IOException("EOF in the middle of table name");
                }
                else if (b == '`') {
                    addToField(b);
                    state = State.INSERT_TABLE;
                }
                else if (b == '\\') {
                    addToField(b);
                    b = inputStream.read();
                    if (b >= 0)
                        addToField(b);
                }
                break;
            case NEXT_ROW_CTOR:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == '(') {
                    newRow();
                    state = State.NEXT_FIELD;
                }
                else if (b == ';') {
                    state = State.STATEMENT_START;
                }
                else {
                    throw new IOException("Unexpected token " + (char)b);
                }
                break;
            case AFTER_ROW_CTOR:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == ';') {
                    state = State.STATEMENT_START;
                }
                else if (b == ',') {
                    state = State.NEXT_ROW_CTOR;
                }
                else {
                    throw new IOException("Unexpected token " + (char)b);
                }
                break;
            case NEXT_FIELD:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == ')') {
                    state = State.AFTER_ROW_CTOR;
                    return row;
                }
                else if (b == '\'') {
                    state = State.QUOTED_FIELD;
                }
                else if (b == ',') {
                    addField(false);
                }
                else if ((b == ' ') || (b == '\r') || (b == '\n')) {
                }
                else {
                    addToField(b);
                    state = State.UNQUOTED_FIELD;
                }
                break;
            case UNQUOTED_FIELD:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == ',') {
                    addField(false);
                    state = State.NEXT_FIELD;
                }
                else if (b == ')') {
                    addField(false);
                    state = State.AFTER_ROW_CTOR;
                    return row;
                }
                else if (b == '\'') {
                    throw new IOException("Quote in the middle of a value");
                }
                else {
                    addToField(b);
                }
                break;
            case QUOTED_FIELD:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a quote");
                }
                else if (b == '\'') {
                    state = State.AFTER_QUOTED_FIELD;
                }
                else if (b == '\\') {
                    b = inputStream.read();
                    switch (b) {
                    case -1:
                        throw new IOException("EOF in the middle of a quote");
                    case 'n':
                        b = '\n';
                        break;
                    case 'r':
                        b = '\r';
                        break;
                    case 't':
                        b = '\t';
                        break;
                    }
                    addToField(b);
                }
                else {
                    addToField(b);
                }
                break;
            case AFTER_QUOTED_FIELD:
                if (b < 0) {
                    throw new IOException("EOF in the middle of a statement");
                }
                else if (b == ',') {
                    addField(true);
                    state = State.NEXT_FIELD;
                }
                else if (b == ')') {
                    addField(true);
                    state = State.AFTER_ROW_CTOR;
                    return row;
                }
                else {
                    throw new IOException("unexpected after quoted field: " + (char)b);
                }
                break;
            }
        }
    }

}
