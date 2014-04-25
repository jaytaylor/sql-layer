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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/** Read from a flat file into <code>NewRow</code> rows suitable for inserting. */
public class CsvRowReader extends RowReader
{
    private final int delim, quote, escape, nl, cr;
    private enum State { ROW_START, FIELD_START, IN_FIELD, IN_QUOTE, AFTER_QUOTE };
    private State state;

    public CsvRowReader(Table table, List<Column> columns, InputStream inputStream,
                        CsvFormat format, QueryContext queryContext, TypesTranslator typesTranslator) {
        super(table, columns, inputStream, format.getEncoding(), format.getNullBytes(), 
              queryContext, typesTranslator);
        this.delim = format.getDelimiterByte();
        this.quote = format.getQuoteByte();
        this.escape = format.getEscapeByte();
        this.nl = format.getNewline();
        this.cr = format.getReturn();
    }

    public void skipRows(long nrows) throws IOException {
        while (true) {
            int b = read();
            if (b < 0) break;
            if (b == nl) {
                nrows--;
                if (nrows <= 0) break;
            }
        }        
    }

    @Override
    public NewRow nextRow() throws IOException {
        {
            int b = read();
            if (b < 0) return null;
            unread(b);
        }
        newRow();
        state = State.ROW_START;
        while (true) {
            int b = read();
            switch (state) {
            case ROW_START:
                if (b < 0) {
                    return null;
                }
                else if ((b == cr) || (b == nl)) {
                    continue;
                }
                else if (b == delim) {
                    addField(false);
                    state = State.FIELD_START;
                }
                else if (b == quote) {
                    state = State.IN_QUOTE;
                }
                else {
                    addToField(b);
                    state = State.IN_FIELD;
                }
                break;
            case FIELD_START:
                if ((b < 0) || (b == cr) || (b == nl)) {
                    addField(false);
                    return finishRow();
                }
                else if (b == delim) {
                    addField(false);
                }
                else if (b == quote) {
                    state = State.IN_QUOTE;
                }
                else {
                    addToField(b);
                    state = State.IN_FIELD;
                }
                break;
            case IN_FIELD:
                if ((b < 0) || (b == cr) || (b == nl)) {
                    addField(false);
                    return finishRow();
                }
                else if (b == delim) {
                    addField(false);
                    state = State.FIELD_START;
                }
                else if (b == quote) {
                    throw new ExternalRowReaderException("QUOTE in the middle of a field");
                }
                else {
                    addToField(b);
                }
                break;
            case IN_QUOTE:
                if (b < 0)
                    throw new ExternalRowReaderException("EOF inside QUOTE");
                else if (b == quote) {
                    if (escape == quote) {
                        // Must be doubled; peek next character.
                        b = read();
                        if (b == quote) {
                            addToField(b);
                            continue;
                        }
                        else {
                            unread(b);
                        }
                    }
                    state = State.AFTER_QUOTE;
                }
                else if (b == escape) {
                    // Non-doubling escape.
                    b = read();
                    if (b < 0) throw new ExternalRowReaderException("EOF after ESCAPE");
                    addToField(b);
                }
                else {
                    addToField(b);
                }
                break;
            case AFTER_QUOTE:
                if ((b < 0) || (b == cr) || (b == nl)) {
                    addField(true);
                    return finishRow();
                }
                else if (b == delim) {
                    addField(true);
                    state = State.FIELD_START;
                }
                else {
                    throw new ExternalRowReaderException("junk after quoted field");
                }
                break;
            }
        }
    }

}
