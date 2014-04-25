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
import com.foundationdb.server.types.TAggregatorBase;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;

public class MMinMaxAggregation extends TAggregatorBase {

    private final MType mType;
    
    private enum MType {
        MIN() {
            @Override
            boolean condition(int a) {
                return a < 0;
            }   
        }, 
        MAX() {
            @Override
            boolean condition(int a) {
                return a > 0;
            }
        };
        abstract boolean condition (int a);
    }

    public static final TAggregator MIN = new MMinMaxAggregation(MType.MIN);
    public static final TAggregator MAX = new MMinMaxAggregation(MType.MAX);
    
    private MMinMaxAggregation(MType mType) {
        super(mType.name(), null);
        this.mType = mType;
    }

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
        if (source.isNull())
            return;
        if (!state.hasAnyValue()) {
            ValueTargets.copyFrom(source, state);
            return;
        }
        TClass tClass = type.typeClass();
        assert stateType.typeClass().equals(tClass) : "incompatible types " + type + " and " + stateType;
        int comparison = TClass.compare(type, source, stateType, state);
        if (mType.condition(comparison))
            ValueTargets.copyFrom(source, state);
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putNull();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }
}
