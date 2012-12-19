/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.server;

import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.std.TExpressionExplainer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public abstract class ServerJavaRoutineTExpression implements TPreparedExpression {
    protected final Routine routine;
    private final List<? extends TPreparedExpression> inputs;

    protected ServerJavaRoutineTExpression(Routine routine,
                                           List<? extends TPreparedExpression> inputs) {
        this.routine = routine;
        this.inputs = inputs;
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext context) {
        List<TPreptimeValue> values = new ArrayList<TPreptimeValue>(inputs.size());
        boolean allConstant = true, anyNull = false;
        PValueSource constantSource = null;
        for (TPreparedExpression input : inputs) {
            TPreptimeValue value = input.evaluateConstant(context);
            values.add(value);
            if (value.value() == null) {
                allConstant = false;
            }
            else if (value.value().isNull()) {
                anyNull = true;
            }
            if (allConstant && routine.isDeterministic()) {
                ValueRoutineInvocation invocation = new TPreptimeValueRoutineInvocation(routine, values);
                ServerJavaRoutine javaRoutine = javaRoutine = javaRoutine((ServerQueryContext)context, invocation);
                evaluate(javaRoutine);
                constantSource = invocation.getReturnValue();
            }
            if (anyNull && !routine.isCalledOnNullInput()) {
                constantSource = PValueSources.getNullSource(resultType().typeClass().underlyingType());
            }
        }
        return new TPreptimeValue(resultType(), constantSource);
    }

    @Override
    public TInstance resultType() {
        return routine.getReturnValue().tInstance();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer ex = new TExpressionExplainer(Type.FUNCTION, routine.getName().toString(), context);
        for (TPreparedExpression input : inputs) {
            ex.addAttribute(Label.OPERAND, input.getExplainer(context));
        }
        return ex;
    }

    @Override
    public TEvaluatableExpression build() {
        List<TEvaluatableExpression> evals = new ArrayList<TEvaluatableExpression>(inputs.size());
        for (TPreparedExpression input : inputs) {
            evals.add(input.build());
        }
        return new TEvaluatableJavaRoutine(routine, evals);
    }

    protected abstract ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                                     ServerRoutineInvocation invocation);

    protected void evaluate(ServerJavaRoutine javaRoutine) {
        javaRoutine.push();
        boolean success = false;
        try {
            javaRoutine.setInputs();
            javaRoutine.invoke();
            javaRoutine.getOutputs();
            success = true;
        }
        finally {
            javaRoutine.pop(success);
        }
    }

    static abstract class ValueRoutineInvocation extends ServerRoutineInvocation {
        protected PValue returnValue;

        protected ValueRoutineInvocation(Routine routine) {
            super(routine);
            returnValue = new PValue(routine.getReturnValue().tInstance().typeClass());
        }

        public PValueSource getReturnValue() {
            return returnValue;
        }
    }

    static abstract class ValueInvocationValues extends ServerJavaValues {
        private Routine routine;
        private ServerQueryContext context;
        private PValue returnValue;
        
        protected ValueInvocationValues(Routine routine,
                                        ServerQueryContext context,
                                        PValue returnValue) {
            this.routine = routine;
            this.context = context;
            this.returnValue = returnValue;
        }

        @Override
        protected ServerQueryContext getContext() {
            return context;
        }

        @Override
        protected ValueSource getValue(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AkType getAkType(int index) {
            Parameter parameter;
            if (index == RETURN_VALUE_INDEX)
                parameter = routine.getReturnValue();
            else
                parameter = routine.getParameters().get(index);
            return parameter.getType().akType();
        }

        @Override
        protected TInstance getTInstance(int index) {
            Parameter parameter;
            if (index == RETURN_VALUE_INDEX)
                parameter = routine.getReturnValue();
            else
                parameter = routine.getParameters().get(index);
            return parameter.tInstance();
        }

        @Override
        protected void setValue(int index, ValueSource source, AkType akType) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void setPValue(int index, PValueSource source) {
            assert (index == RETURN_VALUE_INDEX);
            if (source == null)
                returnValue.putNull();
            else
                PValueTargets.copyFrom(source, returnValue);
        }

        @Override
        protected ResultSet toResultSet(int index, Object resultSet) {
            throw new UnsupportedOperationException();
        }
    }

    static class TPreptimeValueRoutineInvocation extends ValueRoutineInvocation {
        private List<TPreptimeValue> inputs;

        public TPreptimeValueRoutineInvocation(Routine routine,
                                               List<TPreptimeValue> inputs) {
            super(routine);
            this.inputs = inputs;
        }

        @Override
        public ServerJavaValues asValues(ServerQueryContext context) {
            return new ValueInvocationValues(getRoutine(), context, returnValue) {
                    @Override
                    protected int size() {
                        return inputs.size();
                    }

                    @Override
                    protected PValueSource getPValue(int index) {
                        return inputs.get(index).value();
                    }
                };
        }
    }

    static class TEvaluatableValueRoutineInvocation extends ValueRoutineInvocation {
        private List<TEvaluatableExpression> inputs;

        public TEvaluatableValueRoutineInvocation(Routine routine,
                                                  List<TEvaluatableExpression> inputs) {
            super(routine);
            this.inputs = inputs;
        }

        @Override
        public ServerJavaValues asValues(ServerQueryContext context) {
            return new ValueInvocationValues(getRoutine(), context, returnValue) {
                    @Override
                    protected int size() {
                        return inputs.size();
                    }

                    @Override
                    protected PValueSource getPValue(int index) {
                        return inputs.get(index).resultValue();
                    }
                };
        }
    }

    class TEvaluatableJavaRoutine implements TEvaluatableExpression {
        private Routine routine;
        private List<TEvaluatableExpression> inputs;
        private ValueRoutineInvocation invocation;
        private ServerJavaRoutine javaRoutine;

        public TEvaluatableJavaRoutine(Routine routine,
                                       List<TEvaluatableExpression> inputs) {
            this.routine = routine;
            this.inputs = inputs;
        }

        @Override
        public void with(Row row) {
            for (TEvaluatableExpression input : inputs) {
                input.with(row);
            }
        }

        @Override
        public void with(QueryContext context) {
            for (TEvaluatableExpression input : inputs) {
                input.with(context);
            }
            invocation = new TEvaluatableValueRoutineInvocation(routine, inputs);
            javaRoutine = javaRoutine((ServerQueryContext)context, invocation);
        }

        @Override
        public PValueSource resultValue() {
            return invocation.getReturnValue();
        }

        @Override
        public void evaluate() {
            for (TEvaluatableExpression input : inputs) {
                input.evaluate();
            }
            ServerJavaRoutineTExpression.this.evaluate(javaRoutine);
        }
    }

}
