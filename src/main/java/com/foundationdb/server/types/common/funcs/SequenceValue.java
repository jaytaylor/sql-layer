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

import com.foundationdb.ais.model.Sequence;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;

public class SequenceValue extends TScalarBase {

    public static TScalar[] create(TClass stringType, TClass longType) {
        return new TScalar[] {
            new SequenceValue(false, stringType, longType, 0),
            new SequenceValue(false, stringType, longType, 0, 1),
            new SequenceValue(true, stringType, longType, 0),
            new SequenceValue(true, stringType, longType, 0, 1)
        };
    }

    protected final boolean nextValue;
    protected final TClass inputType, outputType;
    protected final int[] covering;
    
    private static final Logger logger = LoggerFactory.getLogger(SequenceValue.class);

    private SequenceValue (boolean nextValue, TClass inputType, TClass outputType, 
                           int... covering) {
        this.nextValue = nextValue;
        this.inputType = inputType;
        this.outputType = outputType;
        this.covering = covering;
    }

    @Override
    public String displayName() {
        return nextValue ? "NEXTVAL" : "CURRVAL";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(outputType);
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
        return true;
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String[] parts = { "", "" };
        if (covering.length > 1) {
            parts[0] = inputs.get(0).getString();
            parts[1] = inputs.get(1).getString();
        }
        else {
            TableName name = TableName.parse("", inputs.get(0).getString());
            parts[0] = name.getSchemaName();
            parts[1] = name.getTableName();
        }
        if (parts[0].isEmpty()) {
            parts[0] = context.getCurrentSchema();
        }

        TableName sequenceName = new TableName(parts[0], parts[1]);
        StoreAdapter store = context.getQueryContext().getStore();
        Sequence sequence = store.getSequence(sequenceName);
        long value = nextValue ?
            store.sequenceNextValue(sequence) :
            store.sequenceCurrentValue(sequence);

        logger.debug("Sequence loading : {} -> {}", sequenceName, value);

        output.putInt64(value);
    }
}
