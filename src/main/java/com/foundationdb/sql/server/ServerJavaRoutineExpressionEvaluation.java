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

package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.std.AbstractCompositeExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.pvalue.PValueSource;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public abstract class ServerJavaRoutineExpressionEvaluation extends AbstractCompositeExpressionEvaluation {
    protected Routine routine;
    private ServerJavaRoutine javaRoutine;

    protected ServerJavaRoutineExpressionEvaluation(Routine routine,
                                                    List<? extends ExpressionEvaluation> children) {
        super(children);
        this.routine = routine;
    }

    @Override
    public ValueSource eval() {
        if (javaRoutine == null) {
            valueHolder().expectType(routine.getReturnValue().getType().akType());
            RoutineInvocation invocation = new RoutineInvocation(routine, children(), valueHolder());
            javaRoutine = javaRoutine((ServerQueryContext)queryContext(), queryBindings(), invocation);
        }
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
        return valueHolder();
    }

    protected abstract ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                                     QueryBindings bindings,
                                                     ServerRoutineInvocation invocation);

    static class RoutineInvocation extends ServerRoutineInvocation {
        private List<? extends ExpressionEvaluation> inputs;
        private ValueHolder returnValue;

        protected RoutineInvocation(Routine routine,
                                    List<? extends ExpressionEvaluation> inputs,
                                    ValueHolder returnValue) {
            super(routine);
            this.inputs = inputs;
            this.returnValue = returnValue;
        }

        @Override
        public ServerJavaValues asValues(ServerQueryContext context, QueryBindings bindings) {
            return new InvocationValues(getRoutine(), inputs, returnValue, context, bindings);
        }
    }

    static class InvocationValues extends ServerJavaValues {
        private Routine routine;
        private List<? extends ExpressionEvaluation> inputs;
        private ValueHolder returnValue;
        private ServerQueryContext context;
        private QueryBindings bindings;

        protected InvocationValues(Routine routine,
                                   List<? extends ExpressionEvaluation> inputs,
                                   ValueHolder returnValue,
                                   ServerQueryContext context,
                                   QueryBindings bindings) {
            this.routine = routine;
            this.inputs = inputs;
            this.returnValue = returnValue;
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        protected int size() {
            return inputs.size();
        }

        @Override
        protected ServerQueryContext getContext() {
            return context;
        }

        @Override
        protected ValueSource getValue(int index) {
            return inputs.get(index).eval();
        }

        @Override
        protected PValueSource getPValue(int index) {
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
            throw new UnsupportedOperationException();
        }

        @Override
        protected void setValue(int index, ValueSource source, AkType akType) {
            assert (index == RETURN_VALUE_INDEX);
            Converters.convert(source, returnValue);
        }

        @Override
        protected void setPValue(int index, PValueSource source) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected ResultSet toResultSet(int index, Object resultSet) {
            throw new UnsupportedOperationException();
        }
    }

}
