/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.texpressions.TScalarBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.TableName;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;

public class SequenceCurrentValue extends TScalarBase {

    public static final TScalar INSTANCE = new SequenceCurrentValue(MNumeric.BIGINT);

    protected final TClass inputType;
    
    private static final Logger logger = LoggerFactory.getLogger(SequenceCurrentValue.class);

    private SequenceCurrentValue (TClass returnType) {
        this.inputType = returnType;
    }

    @Override
    public String displayName() {
        return "CURRVAL";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputType);
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0, 1);
    }

    @Override
    protected boolean neverConstant() {
        return true;
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String schema = inputs.get(0).getString();
        String sequence = inputs.get(1).getString();
        logger.debug("Sequence loading : {}.{}", schema, sequence);

        TableName sequenceName = new TableName (schema, sequence);
        
        long value = context.sequenceCurrentValue(sequenceName);
        
        output.putInt64(value);
    }
}
