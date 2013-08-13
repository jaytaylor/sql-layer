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

package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.*;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.List;

public class ZNearExpression extends AbstractCompositeExpression {

    @Scalar("znear")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer() {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            int size = argumentTypes.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            
            for (int i = 0; i < size; ++i) {
                argumentTypes.setType(i, AkType.DECIMAL);
            }
            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList) {
            int size = arguments.size();
            if (size != 4) throw new WrongExpressionArityException(4, size);
            return new ZNearExpression(arguments);     
        }

        @Override
        public ExpressionComposer.NullTreating getNullTreating() {
            return ExpressionComposer.NullTreating.RETURN_NULL;        
        }
    };
    
    private static class CompositeEvaluation extends AbstractCompositeExpressionEvaluation {        
        public CompositeEvaluation (List<? extends ExpressionEvaluation> childrenEvals) {
            super(childrenEvals);
        }
        
        @Override
        public ValueSource eval() {
            throw new UnsupportedSQLException("This query is not supported by Akiban, its definition " + 
                                              "is used solely for optimization purposes.");
        }
    }
    
    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new CompositeEvaluation(childrenEvaluations());
    }

    @Override
    public String name() {
        return "ZNEAR";
    }
    
    private static List<? extends Expression> checkArity(List<? extends Expression> children) {
        int size = children.size();
        if (size != 4) throw new WrongExpressionArityException(4, size);
        return children;
    }
    
    protected ZNearExpression(List<? extends Expression> children) {
        super(AkType.LONG, checkArity(children));
    }

}
