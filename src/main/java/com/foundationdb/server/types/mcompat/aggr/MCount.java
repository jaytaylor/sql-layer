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
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.Collections;
import java.util.List;

public class MCount extends TAggregatorBase {

    public static final TAggregator[] INSTANCES = {
            new MCount("count(*)", true, true),
            new MCount("count(*)", true, false),
            new MCount("count", false, true),
            new MCount("count", false, false)
    };

    @Override
    public void input(TInstance type, ValueSource source, TInstance stateType, Value state, Object o) {
        if (countIfNull || (!source.isNull())) {
            long count = state.hasAnyValue() ? state.getInt64() : 0;
            ++count;
            state.putInt64(count);
        }
    }

    @Override
    public List<TInputSet> inputSets() {
        return claimNoInputs ? Collections.<TInputSet>emptyList() : super.inputSets();
    }

    @Override
    public void emptyValue(ValueTarget state) {
        state.putInt64(0L);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT);
    }

    private MCount(String name, boolean countIfNull, boolean claimNoInputs) {
        super(name, null);
        this.countIfNull = countIfNull;
        this.claimNoInputs = claimNoInputs;
    }

    private final boolean countIfNull;
    /**
     * Whether the inputSets() list should be empty. The optimizer sometimes doesn't have an operand for COUNT or
     * COUNT(*), so we get around this by creating two copies of each overload, one which says it has no inputs.
     * By assemble time, both will actually have an input.
     */
    private final boolean claimNoInputs;
}
