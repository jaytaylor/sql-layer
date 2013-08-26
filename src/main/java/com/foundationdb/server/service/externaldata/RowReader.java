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
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.persistitadapter.PValueRowDataCreator;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.t3expressions.T3RegistryService;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types3.ErrorHandlingMode;
import com.foundationdb.server.types3.TCast;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;
import com.foundationdb.server.types3.texpressions.TCastExpression;
import com.foundationdb.server.types3.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;
import com.foundationdb.server.types3.texpressions.TPreparedFunction;
import com.foundationdb.server.types3.texpressions.TPreparedLiteral;
import com.foundationdb.server.types3.texpressions.TValidatedScalar;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Read rows from an external source. */
public abstract class RowReader
{
    private final RowDef rowDef;
    private final int[] fieldColumns; // fieldIndex -> columnIndex
    private final boolean[] nullable; // indexed by field index
    private final int[] constColumns, evalColumns;
    private final byte[] nullBytes;
    private final int tableId;
    private final PValue pstring;
    private final PValue[] pvalues; // indexed by column index
    private final PValueRowDataCreator pvalueCreator;
    private final TExecutionContext[] executionContexts; // indexed by column index
    private final TEvaluatableExpression[] expressions; // indexed by field index
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
        this.fieldColumns = new int[columns.size()];
        this.nullable = new boolean[fieldColumns.length];
        for (int i = 0; i < fieldColumns.length; i++) {
            Column column = columns.get(i);
            fieldColumns[i] = column.getPosition();
            nullable[i] = column.getNullable();
        }
        List<Column> defaultColumns = new ArrayList<>();
        List<Column> functionColumns = new ArrayList<>();
        for (Column column : table.getColumns()) {
            if (columns.contains(column)) continue;
            if (column.getIdentityGenerator() != null) {
                functionColumns.add(column);
            }
            else if (column.getDefaultValue() != null) {
                defaultColumns.add(column);
            }
            else if (column.getDefaultFunction() != null) {
                functionColumns.add(column);
            }
        }
        this.constColumns = new int[defaultColumns.size()];
        for (int i = 0; i < constColumns.length; i++) {
            constColumns[i] = defaultColumns.get(i).getPosition();
        }
        this.evalColumns = new int[functionColumns.size()];
        for (int i = 0; i < evalColumns.length; i++) {
            evalColumns[i] = functionColumns.get(i).getPosition();
        }
        this.pstring = new PValue(MString.VARCHAR.instance(Integer.MAX_VALUE, false));
        this.pvalues = new PValue[rowDef.getFieldCount()];
        this.executionContexts = new TExecutionContext[pvalues.length];
        List<TInstance> inputs = Collections.singletonList(pstring.tInstance());
        for (int fi = 0; fi < fieldColumns.length; fi++) {
            int ci = fieldColumns[fi];
            TInstance output = columns.get(fi).tInstance();
            pvalues[ci] = new PValue(output);
            // TODO: Only needed until every place gets type from
            // PValueTarget, when there can just be one
            // TExecutionContext wrapping the QueryContext.
            executionContexts[ci] = new TExecutionContext(null, 
                                                          inputs, output, queryContext,
                                                          ErrorHandlingMode.WARN,
                                                          ErrorHandlingMode.WARN,
                                                          ErrorHandlingMode.WARN);
        }
        for (int fi = 0; fi < constColumns.length; fi++) {
            int ci = constColumns[fi];
            Column column = defaultColumns.get(fi);
            TInstance output = column.tInstance();
            PValue pvalue = new PValue(output);
            TExecutionContext te = new TExecutionContext(null, 
                                                         inputs, output, queryContext,
                                                         ErrorHandlingMode.WARN,
                                                         ErrorHandlingMode.WARN,
                                                         ErrorHandlingMode.WARN);
            pstring.putString(column.getDefaultValue(), null);
            pvalue.tInstance().typeClass().fromObject(te, pstring, pvalue);
            pvalues[ci] = pvalue;
        }
        this.expressions = new TEvaluatableExpression[evalColumns.length];
        T3RegistryService registry = null;
        if (evalColumns.length > 0) {
            registry = queryContext.getServiceManager().getServiceByClass(T3RegistryService.class);
        }
        for (int fi = 0; fi < evalColumns.length; fi++) {
            int ci = evalColumns[fi];
            Column column = functionColumns.get(fi);
            TInstance columnType = column.tInstance();
            String functionName;
            List<TPreptimeValue> input;
            List<TPreparedExpression> arguments;
            if (column.getIdentityGenerator() != null) {
                Sequence sequence = column.getIdentityGenerator();
                TableName sequenceName = sequence.getSequenceName();
                functionName = "NEXTVAL";
                input = new ArrayList<>(2);
                input.add(PValueSources.fromObject(sequenceName.getSchemaName(), MString.varcharFor(sequenceName.getSchemaName())));
                input.add(PValueSources.fromObject(sequenceName.getTableName(), MString.varcharFor(sequenceName.getTableName())));
                arguments = new ArrayList<>(input.size());
                for (TPreptimeValue tpv : input) {
                    arguments.add(new TPreparedLiteral(tpv.instance(), tpv.value()));
                }
            }
            else {
                functionName = column.getDefaultFunction();
                assert (functionName != null) : column;
                input = Collections.<TPreptimeValue>emptyList();
                arguments = Collections.<TPreparedExpression>emptyList();
            }
            TValidatedScalar overload = registry.getScalarsResolver().get(functionName, input).getOverload();
            TInstance functionType = overload.resultStrategy().fixed(column.getNullable());
            TPreparedExpression expr = new TPreparedFunction(overload, functionType, arguments, queryContext);
            if (!functionType.equals(columnType)) {
                TCast tcast = registry.getCastsResolver().cast(functionType.typeClass(), columnType.typeClass());
                expr = new TCastExpression(expr, tcast, columnType, queryContext);
            }
            TEvaluatableExpression eval = expr.build();
            eval.with(queryContext);
            expressions[fi] = eval;
        }
        this.pvalueCreator = new PValueRowDataCreator();
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
            row.put(fieldColumns[fieldIndex++], null);
            fieldLength = 0;
            return;
        }
        int columnIndex = fieldColumns[fieldIndex];
        // bytes -> string -> parsed typed value -> Java object.
        String string = decodeField();
        pstring.putString(string, null);
        PValue pvalue = pvalues[columnIndex];
        pvalue.tInstance().typeClass()
            .fromObject(executionContexts[columnIndex], pstring, pvalue);
        pvalueCreator.put(pvalue, row, rowDef.getFieldDef(columnIndex), columnIndex);
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

    protected NewRow finishRow() {
        for (int i = 0; i < constColumns.length; i++) {
            int columnIndex = constColumns[i];
            PValueSource pvalue = pvalues[columnIndex];
            pvalueCreator.put(pvalue, row, rowDef.getFieldDef(columnIndex), columnIndex);
        }
        for (int i = 0; i < evalColumns.length; i++) {
            int columnIndex = evalColumns[i];
            TEvaluatableExpression expr = expressions[i];
            expr.evaluate();
            PValueSource pvalue = expr.resultValue();
            pvalueCreator.put(pvalue, row, rowDef.getFieldDef(columnIndex), columnIndex);
        }
        return row;
    }

}
