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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.operator.ArrayBindings;
import com.akiban.qp.operator.Bindings;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.CompareExpression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.VariableExpression;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.Arrays;

class MixedOrderScanStateBounded extends MixedOrderScanState
{
    @Override
    public boolean startScan() throws PersistitException
    {
        Key.Direction direction;
        if (ascending) {
            ValueSource boundarySource =
                cursor.keyRange().lo().boundExpressions(cursor.bindings()).eval(field);
            if (boundarySource.isNull()) {
                cursor.exchange.append(Key.BEFORE);
                direction = Key.GT;
            } else {
                Converters.convert(boundarySource, keyTarget);
                direction = loInclusive ? Key.GTEQ : Key.GT;
            }
        } else {
            ValueSource boundarySource =
                cursor.keyRange().hi().boundExpressions(cursor.bindings()).eval(field);
            if (boundarySource.isNull()) {
                cursor.exchange.append(Key.AFTER);
                direction = Key.LT;
            } else {
                Converters.convert(boundarySource, keyTarget);
                direction = hiInclusive ? Key.LTEQ : Key.LT;
            }
        }
        return cursor.exchange.traverse(direction, false);
    }

    public void setRange(ValueSource lo, ValueSource hi)
    {
        this.loSource = lo;
        this.hiSource = hi;
    }

    public void setRangeLimits(boolean loInclusive, boolean hiInclusive)
    {
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
    }

    public boolean loEqualsHi()
    {
        boolean loEqualsHi = false;
        if (!loSource.isNull() && !hiSource.isNull()) {
            objectTarget.expectType(cursor.keyRange().lo().indexRowType().typeAt(field));
            cursor.bindings().set(0, objectTarget.convertFromSource(loSource));
            objectTarget.expectType(cursor.keyRange().hi().indexRowType().typeAt(field));
            cursor.bindings().set(1, objectTarget.convertFromSource(hiSource));
            loEqualsHi = loEqualsHiEvaluation.eval().getBool();
        }
        return loEqualsHi;
    }

    public MixedOrderScanStateBounded(SortCursorMixedOrder cursor, int field) throws PersistitException
    {
        super(cursor, field, cursor.ordering().ascending(field));
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(cursor.exchange.getKey());
        this.loEqualsHiEvaluation =
            new CompareExpression(
                Arrays.asList(new VariableExpression(cursor.keyRange().lo().indexRowType().typeAt(field), 0),
                              new VariableExpression(cursor.keyRange().hi().indexRowType().typeAt(field), 1)),
                Comparison.EQ).evaluation();
        this.loEqualsHiBindings = new ArrayBindings(2);
        this.loEqualsHiEvaluation.of(this.loEqualsHiBindings);
        this.objectTarget = new ToObjectValueTarget();
    }

    private final PersistitKeyValueTarget keyTarget;
    private final ExpressionEvaluation loEqualsHiEvaluation;
    private final Bindings loEqualsHiBindings;
    private final ToObjectValueTarget objectTarget;
    private ValueSource loSource;
    private ValueSource hiSource;
    private boolean loInclusive;
    private boolean hiInclusive;
}
