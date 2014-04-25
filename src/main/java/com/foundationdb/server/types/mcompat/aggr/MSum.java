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

import com.foundationdb.server.error.OverflowException;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.value.*;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import java.util.List;

public class MSum extends TFixedTypeAggregator {

    private final SumType sumType;
    
    private enum SumType {
        BIGINT(MNumeric.BIGINT) {
            @Override
            void input(TInstance type, ValueSource source, TInstance stateType, Value state) {
                long oldState = source.getInt64();
                long input = state.getInt64();
                long sum = oldState + input;
                if (oldState > 0 && input > 0 && sum <= 0) {
                    throw new OverflowException();
                } else if (oldState < 0 && input < 0 && sum >= 0) {
                    throw new OverflowException();
                } else {
                    state.putInt64(sum);
                }
            }
        }, 
        DOUBLE(MApproximateNumber.DOUBLE) {
            @Override
            void input(TInstance type, ValueSource source, TInstance stateType, Value state) {
                double oldState = source.getDouble();
                double input = state.getDouble();
                double sum = oldState + input;
                if (Double.isInfinite(sum) && !Double.isInfinite(oldState) && !Double.isInfinite(input)) {
                    throw new OverflowException();
                } else {
                    state.putDouble(sum);
                }
            }
        },
        DECIMAL(MNumeric.DECIMAL) {
            @Override
            void input(TInstance type, ValueSource source, TInstance stateType, Value state) {
                BigDecimalWrapper oldState = TBigDecimal.getWrapper(source, type);
                BigDecimalWrapper input = TBigDecimal.getWrapper(state, type);
                state.putObject(oldState.add(input));
            }
        }
        ;
        abstract void input(TInstance type, ValueSource source, TInstance stateType, Value state);
        private final TClass typeClass;
        
        private SumType(TClass typeClass) {
            this.typeClass = typeClass;
        }
    }
    
    public static final TAggregator[] INSTANCES = {
        new MSum(SumType.DECIMAL),
        new MSum(SumType.DOUBLE),
        new MSum(SumType.BIGINT)
    };
    
    private MSum(SumType sumType) {
        super("sum", sumType.typeClass);
        this.sumType = sumType;
    }

    // Want integers to all sum as long and floats as double, but decimals as the
    // particular precision of the input.

    @Override
    public List<TInputSet> inputSets() {
        if (sumType != SumType.DECIMAL)
            return super.inputSets();

        TInputSetBuilder builder = new TInputSetBuilder();
        builder.pickingCovers(inputClass(), 0);
        return builder.toList();
    }
    
    @Override
    public TOverloadResult resultType() {
        if (sumType != SumType.DECIMAL)
            return super.resultType();

        return TOverloadResult.picking();
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
            if (source.isNull())
                return;
        if (!state.hasAnyValue())
            ValueTargets.copyFrom(source, state);
        else
            sumType.input(type, source, stateType, state);
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putNull();
    }
}
