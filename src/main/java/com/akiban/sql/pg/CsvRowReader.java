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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PValueRowDataCreator;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.ErrorHandlingMode;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.List;

public class CsvRowReader
{
    private final RowDef rowDef;
    private final int[] fieldMap;
    private final boolean[] nullable;
    private final CsvFormat format;
    private final int delim, quote, escape, nl, cr;
    private final byte[] nullBytes;
    private final int tableId;
    private final boolean usePValues;
    private final PValue pstring;
    private final PValue[] pvalues;
    private final PValueRowDataCreator pvalueCreator;
    private final TExecutionContext executionContext;
    private final ValueHolder holder;
    private final ToObjectValueTarget toObject;
    private NewRow row;
    private enum State { ROW_START, FIELD_START, IN_FIELD, IN_QUOTE, AFTER_QUOTE };
    private State state;
    private byte[] buffer = new byte[128];
    private int fieldIndex, fieldLength;

    public CsvRowReader(UserTable table, List<Column> columns, CsvFormat format,
                        QueryContext queryContext) {
        this.tableId = table.getTableId();
        this.rowDef = table.rowDef();
        this.fieldMap = new int[columns.size()];
        this.nullable = new boolean[fieldMap.length];
        for (int i = 0; i < fieldMap.length; i++) {
            Column column = columns.get(i);
            fieldMap[i] = column.getPosition();
            nullable[i] = column.getNullable();
        }
        this.format = format;
        this.delim = format.getDelimiterByte();
        this.quote = format.getQuoteByte();
        this.escape = format.getEscapeByte();
        this.nl = format.getNewline();
        this.cr = format.getReturn();
        this.nullBytes = format.getNullBytes();
        this.usePValues = Types3Switch.ON;
        if (usePValues) {
            pstring = new PValue(MString.VARCHAR.instance(Integer.MAX_VALUE, false));
            pvalues = new PValue[columns.size()];
            for (int i = 0; i < pvalues.length; i++) {
                pvalues[i] = new PValue(columns.get(i).tInstance());
            }
            pvalueCreator = new PValueRowDataCreator();
            executionContext = new TExecutionContext(null, null, null, 
                                                     queryContext,
                                                     ErrorHandlingMode.WARN,
                                                     ErrorHandlingMode.WARN,
                                                     ErrorHandlingMode.WARN);
            holder = null;
            toObject = null;
        }
        else {
            holder = new ValueHolder();
            toObject = new ToObjectValueTarget();
            pstring = null;
            pvalues = null;
            pvalueCreator = null;
            executionContext = null;
        }
    }

    public NewRow nextRow(InputStream inputStream) throws IOException {
        row = new NiceRow(tableId, rowDef);
        state = State.ROW_START;
        fieldIndex = fieldLength = 0;
        while (true) {
            int b = inputStream.read();
            switch (state) {
            case ROW_START:
                if ((b == cr) || (b == nl)) {
                    continue;
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
            case FIELD_START:
                if ((b == cr) || (b == nl)) {
                    addField(false);
                    return row;
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
                if ((b == cr) || (b == nl)) {
                    addField(false);
                    return row;
                }
                else if (b == delim) {
                    addField(false);
                }
                else if (b == quote) {
                    throw new IOException("QUOTE in the middle of a field");
                }
                else {
                    addToField(b);
                }
                break;
            case IN_QUOTE:
                if (b < 0)
                    throw new IOException("EOF inside QUOTE");
                if (b == escape) {
                    b = inputStream.read();
                    if (b < 0) throw new IOException("EOF after ESCAPE");
                    addToField(b);
                }
                else if (b == quote) {
                    state = State.AFTER_QUOTE;
                }
                else {
                    addToField(b);
                }
                break;
            case AFTER_QUOTE:
                if (b == delim) {
                    addField(true);
                }
                else if ((b == cr) || (b == nl)) {
                    addField(true);
                    return row;
                }
                else {
                    throw new IOException("junk after quoted field");
                }
                break;
            }
        }
    }

    private void addToField(int b) {
        if (fieldLength + 1 > buffer.length) {
            buffer = Arrays.copyOf(buffer, (buffer.length * 3) / 2);
        }
        buffer[fieldLength++] = (byte)b;
    }

    private void addField(boolean quoted) {
        if (!quoted && nullable[fieldIndex]) {
            // Check whether unquoted value matches the representation
            // of null, normally the empty string.
            if (fieldLength == nullBytes.length) {
                boolean match = true;
                for (int i = 0; i < fieldLength; i++) {
                    if (buffer[i] != nullBytes[i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    row.put(fieldMap[fieldIndex++], null);
                    return;
                }
            }
        }
        int columnIndex = fieldMap[fieldIndex];
        // Normal case is: bytes -> string -> parsed typed value -> Java object.
        String string;
        try {
            string = new String(buffer, 0, fieldLength, format.getEncoding());
        }
        catch (UnsupportedEncodingException ex) {
            UnsupportedCharsetException nex = new UnsupportedCharsetException(format.getEncoding());
            nex.initCause(ex);
            throw nex;
        }
        if (usePValues) {
            pstring.putString(string, null);
            PValue pvalue = pvalues[fieldIndex];
            pvalue.tInstance().typeClass().fromObject(executionContext, pstring, pvalue);
            pvalueCreator.put(pvalue, row, rowDef.getFieldDef(columnIndex), columnIndex);
        }
        else {
            holder.putString(string);
            row.put(columnIndex, toObject.convertFromSource(holder));
        }
        fieldIndex++;
    }

}
