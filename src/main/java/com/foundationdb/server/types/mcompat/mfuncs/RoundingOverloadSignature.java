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
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;

import java.util.List;

enum RoundingOverloadSignature {
    ONE_ARG {
        @Override
        protected int roundToScale(LazyList<? extends ValueSource> inputs) {
            return 0;
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0);
        }

        @Override
        protected ValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return ZERO;
        }

        private final ValueSource ZERO = new Value(MNumeric.INT.instance(false), 0);
    },
    TWO_ARGS {
        @Override
        protected int roundToScale(LazyList<? extends ValueSource> inputs) {
            return inputs.get(1).getInt32();
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0).covers(MNumeric.INT, 1);
        }

        @Override
        protected ValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return inputs.get(1).value();
        }
    };

    protected abstract int roundToScale(LazyList<? extends ValueSource> inputs);
    protected abstract void buildInputSets(TClass arg0, TInputSetBuilder builder);
    protected abstract ValueSource getScaleOperand(List<? extends TPreptimeValue> inputs);
}
