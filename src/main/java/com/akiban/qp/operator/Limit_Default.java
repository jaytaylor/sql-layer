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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;
import com.akiban.server.error.NegativeLimitException;

import java.util.Collections;
import java.util.List;
import java.util.Set;

final class Limit_Default extends Operator
{

    // Operator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter) {
        return new Execution(adapter, inputOperator.cursor(adapter));
    }

    // Plannable interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan() {
        return super.describePlan();
    }

    // Object interface

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append("(");
        if (skip > 0) {
            str.append(String.format("skip=%d", skip));
        }
        if ((limit >= 0) && (limit < Integer.MAX_VALUE)) {
            str.append(String.format("limit=%d", limit));
        }
        str.append(": ");
        str.append(inputOperator);
        str.append(")");
        return str.toString();
    }

    // Limit_Default interface

    Limit_Default(Operator inputOperator, int limit) {
        this(inputOperator, 0, false, limit, false);
    }

    Limit_Default(Operator inputOperator,
                  int skip, boolean skipIsBinding,
                  int limit, boolean limitIsBinding) {
        ArgumentValidation.isGTE("skip", skip, 0);
        ArgumentValidation.isGTE("limit", limit, 0);
        this.skip = skip;
        this.skipIsBinding = skipIsBinding;
        this.limit = limit;
        this.limitIsBinding = limitIsBinding;
        this.inputOperator = inputOperator;
    }

    public int skip() {
        return skip;
    }
    public boolean isSkipBinding() {
        return skipIsBinding;
    }
    public int limit() {
        return limit;
    }
    public boolean isLimitBinding() {
        return limitIsBinding;
    }

    // Object state

    private final int skip, limit;
    private final boolean skipIsBinding, limitIsBinding;
    private final Operator inputOperator;

    // internal classes

    private class Execution extends ChainedCursor {

        // Cursor interface

        @Override
        public void open(Bindings bindings) {
            super.open(bindings);
            if (isSkipBinding()) {
                Integer i = (Integer)bindings.get(skip());
                if (i != null)
                    this.skipLeft = i.intValue();
            }
            else {
                this.skipLeft = skip();
            }
            if (skipLeft < 0)
                throw new NegativeLimitException("OFFSET", skipLeft);
            if (isLimitBinding()) {
                Integer i = (Integer)bindings.get(limit());
                if (i == null)
                    this.limitLeft = Integer.MAX_VALUE;
                else
                    this.limitLeft = i.intValue();
            }
            else {
                this.limitLeft = limit();
            }
            if (limitLeft < 0)
                throw new NegativeLimitException("LIMIT", limitLeft);
        }

        @Override
        public Row next() {
            checkQueryCancelation();
            Row row;
            while (skipLeft > 0) {
                if ((row = input.next()) == null) {
                    skipLeft = 0;
                    limitLeft = -1;
                    return null;
                }
                skipLeft--;
            }
            if (limitLeft < 0) {
                return null;
            }
            if (limitLeft == 0) {
                input.close();
                return null;
            }
            if ((row = input.next()) == null) {
                limitLeft = -1;
                return null;
            }
            --limitLeft;
            return row;
        }

        // Execution interface
        Execution(StoreAdapter adapter, Cursor input) {
            super(adapter, input);
        }

        // class state

        private int skipLeft, limitLeft;
    }
}
