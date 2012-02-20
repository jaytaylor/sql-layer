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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractCompositeExpression implements Expression {

    protected abstract boolean nullIsContaminating ();

    // Expression interface

    @Override
    public Explainer getExplainer ()
    {
        return new ExpressionExplainer(Type.FUNCTION, name(), children);
    }
    
    @Override
    public boolean isConstant() {
        boolean hasNonConst = false;
        for (Expression child : children)
        {
            if (child.valueType() == AkType.NULL && nullIsContaminating()) return true;
            if(!child.isConstant())
            {
                if (!nullIsContaminating()) return false;
                hasNonConst = true;
            }
           
        }
        return !hasNonConst;
    }

    @Override
    public boolean needsRow() {
        boolean needrow = false;
        for (Expression child : children) {
            if (child.valueType() == AkType.NULL && nullIsContaminating()) return false;
            if(child.needsRow()) 
            {
                if (!nullIsContaminating()) return true;
                needrow = true;
            }
        }
        return needrow;
    }

    @Override
    public boolean needsBindings() {
        boolean needBindings = false;
        for (Expression child : children) {
            if (child.valueType() == AkType.NULL && nullIsContaminating()) return false;
            if(child.needsBindings()) 
            {
                if (!nullIsContaminating()) return true;
                needBindings = true;
            }
        }
        return needBindings;
    }

    @Override
    public final AkType valueType() {
        return type;
    }    

    // for use by subclasses   

    protected final List<? extends Expression> children() {
        return children;
    }

    protected AbstractCompositeExpression(AkType type, Expression... children) {
        this(type, Arrays.asList(children));
    }

    protected AbstractCompositeExpression(AkType type, List<? extends Expression> children) {
        this.children = children.isEmpty()
                ? Collections.<Expression>emptyList()
                : Collections.unmodifiableList(new ArrayList<Expression>(children));
        this.type = type;
    }

    protected List<? extends ExpressionEvaluation> childrenEvaluations() {
        List<ExpressionEvaluation> result = new ArrayList<ExpressionEvaluation>();
        for (Expression expression : children) {
            result.add(expression.evaluation());
        }
        return result;
    }

    protected static AkType childrenType(List<? extends Expression> children) {
        Iterator<? extends Expression> iter = children.iterator();
        if (!iter.hasNext())
            throw new IllegalArgumentException("expression must take at least one operand; none provided");
        AkType type = iter.next().valueType();
        while(iter.hasNext()) { // should only be once, but AbstractTwoArgExpression will check that
            AkType childType = iter.next().valueType();
            if (type == AkType.NULL) {
                type = childType;
            }
            // TODO put this back in when we get casting expressions. Until then, Extractors will do their job.
//            else if (childType != AkType.NULL && !type.equals(childType)) {
//                throw new IllegalArgumentException("Comparison's children must all have same type. First child was "
//                        + type + ", but then saw " + childType);
//            }
        }
        return type;
    }   
    
    // object state

    private final List<? extends Expression> children;
    private final AkType type;    
}
