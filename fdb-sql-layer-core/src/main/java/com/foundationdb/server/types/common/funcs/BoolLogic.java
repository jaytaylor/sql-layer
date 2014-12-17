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

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.google.common.base.Objects;

import java.util.List;

public class BoolLogic extends TScalarBase
{
    public static final TScalar AND = new BoolLogic(Op.AND);
    public static final TScalar OR = new BoolLogic(Op.OR);
    public static final TScalar XOR = new BoolLogic(Op.XOR);

    private static final int OUT_VAL = 0;

    public static final TScalar NOT = new TScalarBase() {
        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(AkBool.INSTANCE, 0);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs,
                                  ValueTarget output) {
            output.putBool(!inputs.get(0).getBoolean());
        }

        @Override
        public String displayName() {
            return "NOT";
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(AkBool.INSTANCE);
        }
    };

    private static enum Op
    {
        AND(Boolean.FALSE),
        OR(Boolean.TRUE),
        XOR(null)
        {
            @Override
            boolean evaluate(boolean first, boolean second) {
                return first ^ second;
            }
        };

        private Op(Boolean contaminant) {
            this.contaminant = contaminant;
        }

        private final Boolean contaminant;

        boolean evaluate(boolean first, boolean second) {
            // this implementation works for both AND and OR.
            // Since AND's contaminant is FALSE, if we get to this method we know first is true.
            // In that case, the result is true iff second is true.
            // Likewise, since OR's contaminant is TRUE, if we get to this method we know first is false, and
            // the result is true iff second is true.
            // This means we'll only ever need to override this method for XOR. Since that's a relatively rare
            // method, hopefully we'll never need it and the JIT can optimize assuming that this method is not
            // overridden.
            return second;
        }
    }
    
    private final Op op;
    
    BoolLogic (Op op)
    {
        this.op = op;
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        CompoundExplainer ex = super.getExplainer(context, inputs, resultType);
        ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance(op.name()));
        return ex;
    }

    @Override
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        // The expression is const iff either argument is a const whose value is equal to op.contaminant.
        // The first argument can never make the expression non-const (though it can make it const), and the second
        // argument can never leave the constness unknown.
        ValueSource preptimeValue = constSource(values, inputIndex);
        if ((preptimeValue != null) && Objects.equal(op.contaminant, getBoolean(preptimeValue)))
        {
            context.set(OUT_VAL, op.contaminant);
            return Constantness.CONST;
        }
        return (inputIndex == 0) ? Constantness.UNKNOWN : Constantness.NOT_CONST;
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false; // we'll deal with contamination ourselves
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        Object outVal = context.preptimeObjectAt(OUT_VAL);
        if (outVal != null)
        {
            output.putBool((Boolean)outVal);
            return;
        }
        Boolean firstArg = getBoolean(inputs, 0);
        final Boolean result;
        if (Objects.equal(op.contaminant, firstArg)) {
            result = firstArg;
        }
        else {
            // need to look at the second arg
            Boolean secondArg = getBoolean(inputs, 1);
            if (Objects.equal(op.contaminant, secondArg)) {
                result = secondArg;
            }
            else if ( (firstArg == null) || (secondArg == null) ) {
                result = null;
            }
            else {
                result = op.evaluate(firstArg, secondArg);
            }
        }
        if (result == null)
            output.putNull();
        else
            output.putBool(result);
    }

    private Boolean getBoolean(LazyList<? extends ValueSource> inputs, int i) {
        return getBoolean(inputs.get(i));
    }

    private Boolean getBoolean(ValueSource firstInput) {
        return firstInput.isNull() ? null : firstInput.getBoolean();
    }


    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(AkBool.INSTANCE, 0, 1);
    }

    @Override
    public String displayName()
    {
        return op.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }
    
}
