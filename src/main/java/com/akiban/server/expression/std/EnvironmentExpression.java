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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.Bindings;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionComposerWithBindingPosition;
import com.akiban.server.types.AkType;
import java.util.List;

public abstract class EnvironmentExpression extends AbstractNoArgExpression
{
    public enum EnvironmentValue {
        CURRENT_DATE,
        CURRENT_USER,
        SESSION_USER,
        SYSTEM_USER
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    @Override
    public String name() {
        return environmentValue().toString();
    }

    // for use by subclasses

    protected EnvironmentValue environmentValue() {
        return environmentValue;
    }

    protected int bindingPosition() {
        return bindingPosition;
    }

    protected EnvironmentExpression(AkType type, 
                                    EnvironmentValue environmentValue,
                                    int bindingPosition) {
        super(type);
        this.environmentValue = environmentValue;
        this.bindingPosition = bindingPosition;
    }

    private final EnvironmentValue environmentValue;
    private final int bindingPosition;

    public static abstract class EnvironmentComposer extends NoArgComposer
                                                     implements ExpressionComposerWithBindingPosition {
        public EnvironmentValue getEnvironmentValue() {
            return environmentValue;
        }

        @Override
        protected Expression compose() {
            throw new AkibanInternalException("Should have been called with binding position");
        }

        // for use by subclasses

        protected EnvironmentValue environmentValue() {
            return environmentValue;
        }

        protected abstract Expression compose(int bindingPosition);

        @Override
        public Expression compose(int bindingPosition, List<? extends Expression> arguments) {
            if (arguments.size() != 0)
                throw new WrongExpressionArityException(0, arguments.size());
            return compose(bindingPosition);
        }
        
        protected EnvironmentComposer(EnvironmentValue environmentValue) {
            this.environmentValue = environmentValue;
        }

        private final EnvironmentValue environmentValue;
    }

    public static abstract class EnvironmentEvaluation extends AbstractNoArgExpressionEvaluation {
        
        @Override
        public void of(Bindings bindings) {
            this.bindings = bindings;
        }

        // for use by subclasses
        
        public Object environmentValue() {
            return bindings.get(bindingsPosition);
        }

        protected EnvironmentEvaluation(int bindingsPosition) {
            this.bindingsPosition = bindingsPosition;
        }

        private Bindings bindings;
        private final int bindingsPosition;
    }

}
