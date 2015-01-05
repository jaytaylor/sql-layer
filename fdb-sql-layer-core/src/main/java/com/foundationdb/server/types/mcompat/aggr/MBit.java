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

import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TFixedTypeAggregator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

public abstract class MBit extends TFixedTypeAggregator {
    
    public static final TAggregator[] INSTANCES = {
        // BIT_AND
        new MBit("BIT_AND") {

            @Override
            long process(long i0, long i1) {
                return i0 & i1;
            }
        }, 
        // BIT_OR
        new MBit("BIT_OR") {

            @Override
            long process(long i0, long i1) {
                return i0 | i1;
            }
        }, 
        // BIT_XOR
        new MBit("BIT_XOR") {

            @Override
            long process(long i0, long i1) {
                return i0 ^ i1;
            }
        }
    };
    
    abstract long process(long i0, long i1);

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
        if (!source.isNull()) {
            long incoming = source.getInt64();
            if (!state.hasAnyValue()) {
                state.putInt64(incoming);
            }
            else {
                long previousState = state.getInt64();
                state.putInt64(process(previousState, incoming));
            }
        }    
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putNull();
    }

    private MBit(String name) {
        super(name, MNumeric.BIGINT);
    }
}
