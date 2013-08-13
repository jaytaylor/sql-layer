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

package com.foundationdb.server.types3.mcompat.aggr;

import com.foundationdb.server.types3.TAggregator;
import com.foundationdb.server.types3.TAggregatorBase;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.pvalue.PValueTargets;

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
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object o) {
        if (source.isNull())
            return;
        if (!state.hasAnyValue()) {
            PValueTargets.copyFrom(source, state);
            return;
        }
        TClass tClass = instance.typeClass();
        assert stateType.typeClass().equals(tClass) : "incompatible types " + instance + " and " + stateType;
        int comparison = TClass.compare(instance, source, stateType, state);
        if (mType.condition(comparison))
            PValueTargets.copyFrom(source, state);
    }

    @Override
    public void emptyValue(PValueTarget state) {
        state.putNull();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }
}
