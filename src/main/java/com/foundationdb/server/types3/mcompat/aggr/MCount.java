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

package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TAggregatorBase;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

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
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object o) {
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
    public void emptyValue(PValueTarget state) {
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
