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

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.ColumnName;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

@SuppressWarnings("unused") // reflection
public class SerialSequence extends TScalarBase
{
    public static final String NAME = "SERIAL_SEQUENCE";
    public static final String ALIAS = "IDENTITY_SEQUENCE";

    public static TScalar[] create(TClass varchar) {
        return new TScalar[] {
            new SerialSequence(varchar, varchar, 0),       // ('schema.table.column')
            new SerialSequence(varchar, varchar, 0, 1),    // ('table', 'column')
            new SerialSequence(varchar, varchar, 0, 1, 2), // ('schema', 'table', 'column')
        };
    }

    protected final TClass inputType;
    protected final TClass outputType;
    protected final int[] covering;

    private SerialSequence(TClass inputType, TClass outputType, int... covering) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.covering = covering;
    }

    @Override
    public String displayName() {
        return NAME;
    }

    @Override
    public String[] registeredNames() {
        return new String[] { NAME, ALIAS };
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(outputType, 261);
        //SchemaTablesService.IDENT_MAX * 2 + quotes + period = 261
        // default fixed VARCHAR is 255, which is too short (potentially)
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(inputType, covering);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return true;
    }

    @Override
    protected boolean neverConstant() {
        return false;
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
                              LazyList<? extends ValueSource> inputs,
                              ValueTarget output) {
        String[] parts = { "", "", "" };
        if(covering.length == 1) {
            ColumnName columnName = ColumnName.parse("", inputs.get(0).getString());
            parts[0] = columnName.getTableName().getSchemaName();
            parts[1] = columnName.getTableName().getTableName();
            parts[2] = columnName.getName();
        } else {
            for(int i = covering.length - 1, j = 2; i >= 0; --i, --j) {
                parts[j] = inputs.get(i).getString();
            }
        }
        if(parts[0].isEmpty()) {
            parts[0] = context.getCurrentSchema();
        }
        
        AkibanInformationSchema ais = context.getQueryContext().getAIS();
        Table table = ais.getTable(parts[0], parts[1]);
        if(table == null) {
            throw new NoSuchTableException(parts[0], parts[1]);
        }
        Column column = table.getColumn(parts[2]);
        if(column == null) {
            throw new NoSuchColumnException(String.format("%s.%s.%s", parts[0], parts[1], parts[2]));
        }
        Sequence seq = column.getIdentityGenerator();
        if(seq == null) {
            output.putNull();
        } else {
            output.putString(seq.getSequenceName().toStringEscaped(), null);
        }
    }
}
