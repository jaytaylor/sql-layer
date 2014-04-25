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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

abstract class ContextualEvaluation<T> implements TEvaluatableExpression {

    @Override
    public void with(Row row) {
    }

    @Override
    public void with(QueryContext context) {
    }

    @Override
    public void with(QueryBindings bindings) {
    }

    @Override
    public ValueSource resultValue() {
        if (readyValue == null)
            throw new IllegalStateException("haven't evaluated since having seen a new context");
        return readyValue;
    }

    @Override
    public void evaluate() {
        if (context == null)
            throw new IllegalStateException("no context given");
        if (readyValue == null) {
            if (unreadyValue == null) {
                // first evaluation of this expression
                readyValue = new Value(underlyingType);
            }
            else {
                // readyValue is null, unreadyValue is not null. Means we've seen a QueryContext but have
                // not evaluated it. Set the readyValue to unreadyValue, as we've about to evaluate it.
                readyValue = unreadyValue;
                unreadyValue = null;
            }
        }
        evaluate(context, readyValue);
    }

    protected void setContext(T context) {
        if (unreadyValue == null) {
            // give unreadValue the readyValue, whatever it is (it could be null if we've never evaluated)
            // then set readyValue to null
            unreadyValue = readyValue;
            readyValue = null;
        }
        this.context = context;
    }
    
    protected abstract void evaluate(T context, ValueTarget target);

    protected ContextualEvaluation(TInstance underlyingType) {
        this.underlyingType = underlyingType;
    }

    // At most one of these will be non-null. If no QueryContext has been seen yet, they'll both be null.
    // Otherwise, unreadyValue means we've seen but not evaluated a QueryContext, and readyValue means we've
    // seen a QueryContext and evaluated it.
    private Value unreadyValue;
    private Value readyValue;
    private T context;
    private TInstance underlyingType;
}
