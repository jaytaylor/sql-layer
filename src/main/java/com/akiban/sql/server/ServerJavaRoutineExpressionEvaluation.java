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

package com.akiban.sql.server;

import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.AbstractCompositeExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;

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
            javaRoutine = javaRoutine((ServerQueryContext)queryContext(), invocation);
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
        public ServerJavaValues asValues(ServerQueryContext context) {
            return new InvocationValues(getRoutine(), inputs, returnValue, context);
        }
    }

    static class InvocationValues extends ServerJavaValues {
        private Routine routine;
        private List<? extends ExpressionEvaluation> inputs;
        private ValueHolder returnValue;
        private ServerQueryContext context;

        protected InvocationValues(Routine routine,
                                   List<? extends ExpressionEvaluation> inputs,
                                   ValueHolder returnValue,
                                   ServerQueryContext context) {
            this.routine = routine;
            this.inputs = inputs;
            this.returnValue = returnValue;
            this.context = context;
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
