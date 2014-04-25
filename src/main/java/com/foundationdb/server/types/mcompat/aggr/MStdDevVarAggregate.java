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

package com.foundationdb.server.types.mcompat.aggr;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public class MStdDevVarAggregate extends TFixedTypeAggregator
{
    enum Func {
        // These are the actual aggregate functions. They are only
        // here so that we can distinguish from regular functions
        // early enough for the optimizer to transform them.
        VAR_POP, VAR_SAMP, STDDEV_POP, STDDEV_SAMP,
        // These are the partial aggregators.
        SUM, SUM_SQUARE 
    }

    public static final TAggregator[] INSTANCES = {
        new MStdDevVarAggregate(Func.VAR_POP, "VAR_POP"),
        new MStdDevVarAggregate(Func.VAR_SAMP, "VAR_SAMP"),
        new MStdDevVarAggregate(Func.STDDEV_POP, "STDDEV_POP"),
        new MStdDevVarAggregate(Func.STDDEV_SAMP, "STDDEV_SAMP"),
        new MStdDevVarAggregate(Func.SUM, "_VAR_SUM"),
        new MStdDevVarAggregate(Func.SUM_SQUARE, "_VAR_SUM_2")
    };

    private final Func func;

    private MStdDevVarAggregate(Func func, String name) {
        super(name, MApproximateNumber.DOUBLE);
        this.func = func;
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object del)
    {
        if (source.isNull())
            return;
        double x = source.getDouble();
        double sum = state.hasAnyValue() ? state.getDouble() : 0;
        switch (func) {
        case SUM:
            sum += x;
            break;
        case SUM_SQUARE:
            sum += x * x;
            break;
        default:
            throw new AkibanInternalException("Aggregator for " + displayName() + " should have been optimized out");
        }
        state.putDouble(sum);
    }

    @Override
    public void emptyValue(ValueTarget state)
    {
        state.putNull();
    }
}
