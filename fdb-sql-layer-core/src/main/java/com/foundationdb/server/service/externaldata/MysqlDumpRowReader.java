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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.ExternalRowReaderException;
import com.foundationdb.server.types.common.types.TypesTranslator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    private static final Logger logger = LoggerFactory.getLogger(MysqlDumpRowReader.class);

    public MysqlDumpRowReader(Table table, List<Column> columns,
                              InputStream inputStream, String encoding,
                              QueryContext queryContext, TypesTranslator typesTranslator) {
        super(table, columns, inputStream, encoding, getBytes("NULL", encoding), 
              queryContext, typesTranslator);
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
    public NewRow nextRow() throws IOException {
        while (true) {
            int b = read();
            switch (state) {
            case STATEMENT_START:
                if (b < 0) {
                    return null;
                }
                else if (b == '-') {
                    b = read();
                    if (b == '-') {
                        state = State.SINGLE_LINE_COMMENT;
                    }
                    else {
                        throw unexpectedToken('-', b);
                    }
                }
                else if (b == '/') {
                    b = read();
                    if (b == '*') {
                        state = State.DELIMITED_COMMENT;
                    }
                    else {
                        throw unexpectedToken('/', b);
                    }
                }
                else if ((b >= 'A') && (b <= 'Z')) {
                    addToField(b);
                    state = State.STATEMENT_VERB;
                }
                else if ((b == ' ') || (b == '\r') || (b == '\n')) {
                }
                else {
                    throw unexpectedToken(b);
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
                    throw eofInTheMiddleOf("a comment");
                }
                else if (b == '*') {
                    b = read();
                    if (b == '/') {
                        b = read();
                        if (b != ';')
                            unread(b); // Allow stray ; after comment.
                        state = State.STATEMENT_START;
                    }
                    else {
                        unread(b);
                    }
                }
                break;
            case STATEMENT_VERB:
            case INSERT:
            case INSERT_VALUES:
                if (b < 0) {
                    throw eofInTheMiddleOf("a statement");
                }
                else if ((b >= 'A') && (b <= 'Z')) {
                    addToField(b);
                }
                else {
                    if (b != ' ') unread(b);
                    if (state == State.INSERT) {
                        if (fieldMatches(into)) {
                            clearField();
                            state = State.INSERT_TABLE;
                        }
                        else {
                            throw new ExternalRowReaderException("Unrecognized statement INSERT " + decodeField());
                        }
                    }
                    else if (state == State.INSERT_VALUES) {
                        if (fieldMatches(values)) {
                            clearField();
                            state = State.NEXT_ROW_CTOR;
                        }
                        else {
                            throw new ExternalRowReaderException("Unrecognized statement INSERT INTO " + decodeField());
                        }
                    }
                    else if (fieldMatches(lock) || fieldMatches(unlock)) {
                        clearField();
                        state = State.IGNORED_STATEMENT;
                    }
                    else if (fieldMatches(insert)) {
                        clearField();
                        state = State.INSERT;
                    }
                    else {
                        throw new ExternalRowReaderException("Unrecognized statement " + decodeField());
                    }
                }
                break;
            case IGNORED_STATEMENT:
                if (b < 0) {
                    throw eofInTheMiddleOf("a statement");
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
                    throw eofInTheMiddleOf("a statement");
                }
                else if (b == '`') {
                    state = State.IGNORED_STATEMENT;
                }
                else if (b == '\\') {
                    b = read();
                }
                break;
            case INSERT_TABLE:
                if (b < 0) {
                    throw eofInTheMiddleOf("a statement");
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
                    if (b != ' ') unread(b);
                    if (tableName == null) {
                        tableName = copyField();
                        if (logger.isTraceEnabled()) {
                            logger.trace("Original target table: {}", decodeField());
                        }
                    }
                    else if (!fieldMatches(tableName)) {
                        throw new ExternalRowReaderException("INSERT INTO changed from " + 
                                                             decode(tableName) +
                                                             " to " + decodeField() +
                                                             ". Does file contain multiple tables?");
                    }
                    clearField();
                    state = State.INSERT_VALUES;
                }
                break;
            case TABLE_BACKQUOTE:
                if (b < 0) {
                    throw eofInTheMiddleOf("table name");
                }
                else if (b == '`') {
                    addToField(b);
                    state = State.INSERT_TABLE;
                }
                else if (b == '\\') {
                    addToField(b);
                    b = read();
                    if (b >= 0)
                        addToField(b);
                }
                else {
                    addToField(b);
                }
                break;
            case NEXT_ROW_CTOR:
                if (b < 0) {
                    throw eofInTheMiddleOf("a statement");
                }
                else if (b == '(') {
                    newRow();
                    state = State.NEXT_FIELD;
                }
                else {
                    throw unexpectedToken(b);
                }
                break;
            case AFTER_ROW_CTOR:
                if (b < 0) {
                    throw eofInTheMiddleOf("a statement");
                }
                else if (b == ';') {
                    state = State.STATEMENT_START;
                }
                else if (b == ',') {
                    state = State.NEXT_ROW_CTOR;
                }
                else {
                    throw unexpectedToken(b);
                }
                break;
            case NEXT_FIELD:
                if (b < 0) {
                    throw eofInTheMiddleOf("a statement");
                }
                else if (b == ')') {
                    state = State.AFTER_ROW_CTOR;
                    return finishRow();
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
                    throw eofInTheMiddleOf("a statement");
                }
                else if (b == ',') {
                    addField(false);
                    state = State.NEXT_FIELD;
                }
                else if (b == ')') {
                    addField(false);
                    state = State.AFTER_ROW_CTOR;
                    return finishRow();
                }
                else if (b == '\'') {
                    throw new ExternalRowReaderException("Quote in the middle of a value");
                }
                else {
                    addToField(b);
                }
                break;
            case QUOTED_FIELD:
                if (b < 0) {
                    throw eofInTheMiddleOf("quoted string");
                }
                else if (b == '\'') {
                    state = State.AFTER_QUOTED_FIELD;
                }
                else if (b == '\\') {
                    b = read();
                    switch (b) {
                    case -1:
                        throw eofInTheMiddleOf("quoted string");
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
                    throw eofInTheMiddleOf("a statement");
                }
                else if (b == ',') {
                    addField(true);
                    state = State.NEXT_FIELD;
                }
                else if (b == ')') {
                    addField(true);
                    state = State.AFTER_ROW_CTOR;
                    return finishRow();
                }
                else {
                    throw unexpectedToken(b);
                }
                break;
            }
        }
    }

    protected ExternalRowReaderException unexpectedToken(int... bytes) {
        byte[] ba = new byte[bytes.length];
        for (int i = 0; i < ba.length; i++) {
            ba[i] = (byte)bytes[i];
        }
        return new ExternalRowReaderException("Unexpected token " + decode(ba));
    }

    protected ExternalRowReaderException eofInTheMiddleOf(String what) {
        return new ExternalRowReaderException("EOF in the middle of " + what);
    }

}
