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
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.persistitadapter.PValueRowDataCreator;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types.ToObjectValueTarget;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.ErrorHandlingMode;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.Types3Switch;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Read rows from an external source. */
public abstract class RowReader
{
    private final RowDef rowDef;
    private final int[] fieldMap;
    private final boolean[] nullable;
    private final byte[] nullBytes;
    private final int tableId;
    private final boolean usePValues;
    private final PValue pstring;
    private final PValue[] pvalues;
    private final PValueRowDataCreator pvalueCreator;
    private final TExecutionContext[] executionContexts;
    private final FromObjectValueSource fromObject;
    private final ValueHolder holder;
    private final ToObjectValueTarget toObject;
    private AkType[] aktypes;
    private NewRow row;
    private byte[] fieldBuffer = new byte[128];
    private int fieldIndex, fieldLength;
    private final String encoding;
    private final InputStream inputStream;
    private final byte[] fileBuffer = new byte[1024];
    private int fileIndex, fileAvail;

    protected RowReader(UserTable table, List<Column> columns, 
                        InputStream inputStream, String encoding, byte[] nullBytes,
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
        this.usePValues = Types3Switch.ON;
        if (usePValues) {
            pstring = new PValue(MString.VARCHAR.instance(Integer.MAX_VALUE, false));
            pvalues = new PValue[columns.size()];
            executionContexts = new TExecutionContext[pvalues.length];
            List<TInstance> inputs = Collections.singletonList(pstring.tInstance());
            for (int i = 0; i < pvalues.length; i++) {
                TInstance output = columns.get(i).tInstance();
                pvalues[i] = new PValue(output);
                // TODO: Only needed until every place gets type from
                // PValueTarget, when there can just be one
                // TExecutionContext wrapping the QueryContext.
                executionContexts[i] = new TExecutionContext(null, 
                                                             inputs, output, queryContext,
                                                             ErrorHandlingMode.WARN,
                                                             ErrorHandlingMode.WARN,
                                                             ErrorHandlingMode.WARN);
            }
            pvalueCreator = new PValueRowDataCreator();
            fromObject = null;
            holder = null;
            toObject = null;
            aktypes = null;
        }
        else {
            fromObject = new FromObjectValueSource();
            holder = new ValueHolder();
            toObject = new ToObjectValueTarget();
            aktypes = new AkType[columns.size()];
            for (int i = 0; i < aktypes.length; i++) {
                aktypes[i] = columns.get(i).getType().akType();
            }
            pstring = null;
            pvalues = null;
            pvalueCreator = null;
            executionContexts = null;
        }
        this.inputStream = inputStream;
        this.encoding = encoding;
        this.nullBytes = nullBytes;
    }

    protected NewRow row() {
        return row;
    }

    protected NewRow newRow() {
        row = new NiceRow(tableId, rowDef);
        fieldIndex = fieldLength = 0;
        return row;
    }

    public abstract NewRow nextRow() throws IOException;

    protected int read() throws IOException {
        while (true) {
            if (fileAvail > 0) {
                fileAvail--;
                return fileBuffer[fileIndex++] & 0xFF;
            }
            else if (fileAvail < 0) {
                return -1;
            }
            else {
                fileAvail = inputStream.read(fileBuffer);
                fileIndex = 0;
            }
        }
    }
    
    protected void unread(int b) {
        assert ((fileIndex > 0) && (b == (fileBuffer[fileIndex-1] & 0xFF)));
        fileAvail++;
        fileIndex--;
    }

    protected void addToField(int b) {
        if (fieldLength + 1 > fieldBuffer.length) {
            fieldBuffer = Arrays.copyOf(fieldBuffer, (fieldBuffer.length * 3) / 2);
        }
        fieldBuffer[fieldLength++] = (byte)b;
    }

    protected void addField(boolean quoted) {
        if (!quoted && nullable[fieldIndex] && fieldMatches(nullBytes)) {
            row.put(fieldMap[fieldIndex++], null);
            fieldLength = 0;
            return;
        }
        int columnIndex = fieldMap[fieldIndex];
        // bytes -> string -> parsed typed value -> Java object.
        String string = decodeField();
        if (usePValues) {
            pstring.putString(string, null);
            PValue pvalue = pvalues[fieldIndex];
            pvalue.tInstance().typeClass()
                .fromObject(executionContexts[fieldIndex], pstring, pvalue);
            pvalueCreator.put(pvalue, row, rowDef.getFieldDef(columnIndex), columnIndex);
        }
        else {
            fromObject.setExplicitly(string, AkType.VARCHAR);
            holder.expectType(aktypes[fieldIndex]);
            Converters.convert(fromObject, holder);
            row.put(columnIndex, toObject.convertFromSource(holder));
        }
        fieldIndex++;
        fieldLength = 0;
    }

    protected void clearField() {
        fieldLength = 0;
    }

    protected boolean fieldMatches(byte[] key) {
        // Check whether unquoted value matches the representation
        // of null, normally the empty string.
        if (fieldLength != key.length)
            return false;
        for (int i = 0; i < fieldLength; i++) {
            if (fieldBuffer[i] != key[i]) {
                return false;
            }
        }
        return true;
    }

    protected byte[] copyField() {
        return Arrays.copyOf(fieldBuffer, fieldLength);
    }

    protected String decodeField() {
        try {
            return new String(fieldBuffer, 0, fieldLength, encoding);
        }
        catch (UnsupportedEncodingException ex) {
            UnsupportedCharsetException nex = new UnsupportedCharsetException(encoding);
            nex.initCause(ex);
            throw nex;
        }
    }

    protected String decode(byte[] bytes) {
        try {
            return new String(bytes, encoding);
        }
        catch (UnsupportedEncodingException ex) {
            UnsupportedCharsetException nex = new UnsupportedCharsetException(encoding);
            nex.initCause(ex);
            throw nex;
        }
    }

}
