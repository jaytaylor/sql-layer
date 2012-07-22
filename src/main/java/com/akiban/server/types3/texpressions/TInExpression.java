/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.types3.texpressions;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.ArrayList;
import java.util.List;

public final class TInExpression {

    public static TPreparedExpression prepare(TPreparedExpression lhs, List<? extends TPreparedExpression> rhs) {
        List<TPreparedExpression> all = new ArrayList<TPreparedExpression>(rhs.size() + 1);
        all.add(lhs);
        all.addAll(rhs);
        return new TPreparedFunction(overload, AkBool.INSTANCE.instance(), all);
    }
    
    private static TValidatedOverload overload = new TValidatedOverload(new TOverloadBase() {
        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.vararg(null, 0, 1);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            TInstance lhsInstance = context.inputTInstanceAt(0);
            PValueSource lhsSource = inputs.get(0);
            for (int i=1, nInputs = inputs.size(); i < nInputs; ++i) {
                TInstance rhsInstance = context.inputTInstanceAt(i);
                PValueSource rhsSource = inputs.get(i);
                if (0 == TClass.compare(lhsInstance, lhsSource, rhsInstance, rhsSource)) {
                    output.putBool(true);
                    return;
                }
            }
            output.putBool(false);
        }

        @Override
        public String overloadName() {
            return "in";
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(AkBool.INSTANCE.instance());
        }
    });
}
