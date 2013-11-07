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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

/** Called at the end of a series of partial aggregations for STDDEV / VAR aggregates. */
public class MStdDevVarCalculate extends TScalarBase
{
    enum Func {
        VAR_POP, VAR_SAMP, STDDEV_POP, STDDEV_SAMP
    }
    
    public static final TScalar[] INSTANCES = {
        new MStdDevVarCalculate(Func.VAR_POP, "_VAR_POP"),
        new MStdDevVarCalculate(Func.VAR_SAMP, "_VAR_SAMP"),
        new MStdDevVarCalculate(Func.STDDEV_POP, "_STDDEV_POP"),
        new MStdDevVarCalculate(Func.STDDEV_SAMP, "_STDDEV_SAMP")
    };
        
    private final Func func;
    private final String name;

    private MStdDevVarCalculate(Func func, String name) {
        this.func = func;
        this.name = name;
    }
    
    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MApproximateNumber.DOUBLE, 0, 1);
        builder.covers(MNumeric.BIGINT, 2);
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MApproximateNumber.DOUBLE);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        double sumX2 = inputs.get(0).getDouble();
        double sumX = inputs.get(1).getDouble();
        long count = inputs.get(2).getInt64();
        double result = sumX2 - (sumX * sumX) / count;
        switch (func) {
        case VAR_POP:
        case STDDEV_POP:
        default:
            result = result / count;
            break;
        case VAR_SAMP:
        case STDDEV_SAMP:
            if (count == 1) {
                output.putNull();
                return;
            }
            result = result / (count - 1);
            break;
        }
        switch (func) {
        case STDDEV_POP:
        case STDDEV_SAMP:
            result = Math.sqrt(result);
            break;
        }
        output.putDouble(result);
    }
}
