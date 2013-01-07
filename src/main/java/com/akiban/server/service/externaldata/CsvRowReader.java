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
import com.akiban.server.error.ExternalRowReaderException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/** Read from a flat file into <code>NewRow</code> rows suitable for inserting. */
public class CsvRowReader extends RowReader
{
    private final int delim, quote, escape, nl, cr;
    private enum State { ROW_START, FIELD_START, IN_FIELD, IN_QUOTE, AFTER_QUOTE };
    private State state;

    public CsvRowReader(UserTable table, List<Column> columns, InputStream inputStream,
                        CsvFormat format, QueryContext queryContext) {
        super(table, columns, inputStream, format.getEncoding(), format.getNullBytes(), 
              queryContext);
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
                    return row();
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
                    return row();
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
                    return row();
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
