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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TInputSetBuilder;

import java.util.List;

enum RoundingOverloadSignature {
    ONE_ARG {
        @Override
        protected int roundToScale(LazyList<? extends PValueSource> inputs) {
            return 0;
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0);
        }

        @Override
        protected PValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return ZERO;
        }

        private final PValueSource ZERO = new PValue(MNumeric.INT.instance(false), 0);
    },
    TWO_ARGS {
        @Override
        protected int roundToScale(LazyList<? extends PValueSource> inputs) {
            return inputs.get(1).getInt32();
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0).covers(MNumeric.INT, 1);
        }

        @Override
        protected PValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return inputs.get(1).value();
        }
    };

    protected abstract int roundToScale(LazyList<? extends PValueSource> inputs);
    protected abstract void buildInputSets(TClass arg0, TInputSetBuilder builder);
    protected abstract PValueSource getScaleOperand(List<? extends TPreptimeValue> inputs);
}
