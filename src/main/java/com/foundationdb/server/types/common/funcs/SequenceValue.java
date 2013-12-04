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
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;

public class SequenceValue extends TScalarBase {

    public static final TScalar[] INSTANCES = {
        new SequenceValue(false, MString.VARCHAR, MNumeric.BIGINT, 0),
        new SequenceValue(false, MString.VARCHAR, MNumeric.BIGINT, 0, 1),
        new SequenceValue(true, MString.VARCHAR, MNumeric.BIGINT, 0),
        new SequenceValue(true, MString.VARCHAR, MNumeric.BIGINT, 0, 1)
    };

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
    protected boolean neverConstant() {
        return true;
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String schema = null;
        String sequence;
        if (covering.length > 1) {
            if (!inputs.get(0).isNull()) {
                schema = inputs.get(0).getString();
            }
            sequence = inputs.get(1).getString();
        }
        else {
            sequence = inputs.get(0).getString();
            int idx = sequence.indexOf('.');
            if (idx >= 0) {
                schema = sequence.substring(0, idx);
                sequence = sequence.substring(idx+1);
            }
        }
        if (schema == null) {
            schema = context.getCurrentSchema();
        }
        logger.debug("Sequence loading : {}.{}", schema, sequence);

        TableName sequenceName = new TableName (schema, sequence);
        
        long value = nextValue ? 
            context.sequenceNextValue(sequenceName) : 
            context.sequenceCurrentValue(sequenceName);
        
        output.putInt64(value);
    }
}
