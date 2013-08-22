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

package com.foundationdb.server.expression.std;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.util.ValueHolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractCompositeExpressionEvaluation extends ExpressionEvaluation.Base {

    // ExpressionEvaluation interface

    @Override
    public void of(Row row)
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).of(row);
        }
    }

    @Override
    public void of(QueryContext context)
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).of(context);
        }
        this.context = context;
    }

    @Override
    public void of(QueryBindings bindings)
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).of(bindings);
        }
        this.bindings = bindings;
    }

    @Override
    public void destroy()
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).destroy();
        }
    }

    public AbstractCompositeExpressionEvaluation(List<? extends ExpressionEvaluation> children) {
        this.children = children.isEmpty()
                ? Collections.<ExpressionEvaluation>emptyList()
                : Collections.unmodifiableList(new ArrayList<>(children));
        this.nChildren = this.children.size();
    }

    // Shareable interface

    @Override
    public void acquire()
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).acquire();
        }
    }

    @Override
    public boolean isShared()
    {
        for (int i = 0; i < nChildren; i++) {
            if (children.get(i).isShared()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void release() {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).release();
        }
    }


    // for use by subclasses

    protected final List<? extends ExpressionEvaluation> children() {
        return children;
    }
    
    protected ValueHolder valueHolder() {
        return valueHolder == null ? valueHolder = new ValueHolder() : valueHolder ;
    }
    
    protected QueryContext queryContext() {
        return context;
    }
    
    protected QueryBindings queryBindings() {
        return bindings;
    }
    
    // object state

    private final List<? extends ExpressionEvaluation> children;
    private final int nChildren;
    private ValueHolder valueHolder;
    private QueryContext context;
    private QueryBindings bindings;
}
