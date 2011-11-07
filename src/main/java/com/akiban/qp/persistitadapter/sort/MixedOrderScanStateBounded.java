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
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class MixedOrderScanStateBounded extends MixedOrderScanState
{
    @Override
    public boolean startScan() throws PersistitException
    {
        Key.Direction direction;
        if (ascending) {
            if (loSource.isNull()) {
                cursor.exchange.append(Key.BEFORE);
                direction = Key.GT;
            } else {
                keyTarget.expectingType(loSource.getConversionType());
                Converters.convert(loSource, keyTarget);
                direction = loInclusive ? Key.GTEQ : Key.GT;
            }
            if (!hiSource.isNull()) {
                setupEndComparison(hiInclusive ? Comparison.LE : Comparison.LT, hiSource);
            }
        } else {
            if (hiSource.isNull()) {
                // About null handling: See comment in SortCursorUnidirectional.evaluateBoundaries.
                if (loSource.isNull()) {
                    cursor.exchange.append(null);
                } else {
                    cursor.exchange.append(Key.AFTER);
                }
                direction = Key.LT;
            } else {
                keyTarget.expectingType(hiSource.getConversionType());
                Converters.convert(hiSource, keyTarget);
                direction = hiInclusive ? Key.LTEQ : Key.LT;
            }
            if (!loSource.isNull()) {
                setupEndComparison(loInclusive ? Comparison.GE : Comparison.GT, loSource);
            }
        }
        return cursor.exchange.traverse(direction, false) && !atEnd();
    }

    @Override
    public boolean advance() throws PersistitException
    {
        return super.advance() && !atEnd();
    }

    public void setRange(ValueSource lo, ValueSource hi)
    {
        assert !(lo.isNull() && hi.isNull());
        loSource = lo;
        hiSource = hi;
        this.fieldType = loSource.isNull() ? hiSource.getConversionType() : loSource.getConversionType();
    }

    public void setRangeLimits(boolean loInclusive, boolean hiInclusive)
    {
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
    }

    public MixedOrderScanStateBounded(StoreAdapter adapter, SortCursorMixedOrder cursor, int field)
        throws PersistitException
    {
        super(cursor, field, cursor.ordering().ascending(field));
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(cursor.exchange.getKey());
        this.keySource = new PersistitKeyValueSource();
        this.indexRowType = cursor.keyRange().indexRowType();
    }

    private void setupEndComparison(Comparison comparison, ValueSource bound)
    {
        if (endComparisonExpression == null) {
            keySource.attach(cursor.exchange.getKey(), -1, fieldType); // depth unimportant, will be set later
            endComparisonExpression =
                Expressions.compare(Expressions.valueSource(keySource),
                                    comparison,
                                    Expressions.valueSource(bound));
        }
    }

    private boolean atEnd()
    {
        boolean atEnd;
        if (endComparisonExpression == null) {
            atEnd = false;
        } else {
            // hiComparisonExpression depends on exchange's key, but we need to compare the correct key segment.
            Key key = cursor.exchange.getKey();
            int keyDepth = key.getDepth();
            int keySize = key.getEncodedSize();
            keySource.attach(key, field, fieldType);
            ExpressionEvaluation evaluation = endComparisonExpression.evaluation();
            atEnd = !evaluation.eval().getBool();
            key.setEncodedSize(keySize);
            key.setDepth(keyDepth);
        }
        return atEnd;
    }

    private final IndexRowType indexRowType;
    private final PersistitKeyValueTarget keyTarget;
    private final PersistitKeyValueSource keySource;
    private ValueSource loSource;
    private ValueSource hiSource;
    private boolean loInclusive;
    private boolean hiInclusive;
    private Expression endComparisonExpression;
    private AkType fieldType;
}
