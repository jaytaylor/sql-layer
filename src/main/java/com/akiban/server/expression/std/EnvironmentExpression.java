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
import com.akiban.server.expression.EnvironmentExpressionSetting;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import java.util.List;

public abstract class EnvironmentExpression extends AbstractNoArgExpression
{
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
        return environmentSetting().toString();
    }

    // for use by subclasses

    protected EnvironmentExpressionSetting environmentSetting() {
        return environmentSetting;
    }

    protected int bindingPosition() {
        return bindingPosition;
    }

    protected EnvironmentExpression(AkType type, 
                                    EnvironmentExpressionSetting environmentSetting,
                                    int bindingPosition) {
        super(type);
        this.environmentSetting = environmentSetting;
        this.bindingPosition = bindingPosition;
    }

    private final EnvironmentExpressionSetting environmentSetting;
    private final int bindingPosition;

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
