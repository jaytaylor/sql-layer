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
        // About null handling: See comment in SortCursorUnidirectional.evaluateBoundaries.
        Key.Direction direction;
        if (ascending) {
            if (loSource.isNull()) {
                cursor.exchange.append(null);
                direction = Key.GT;
            } else {
                keyTarget.expectingType(loSource.getConversionType());
                Converters.convert(loSource, keyTarget);
                direction = loInclusive ? Key.GTEQ : Key.GT;
            }
            if (!hiSource.isNull()) {
                setupEndComparison(hiInclusive ? Comparison.LE : Comparison.LT, hiSource);
            }
            // else: endComparison stays null, which causes pastEnd() to always return false.
        } else {
            if (hiSource.isNull()) {
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
        return cursor.exchange.traverse(direction, false) && !pastEnd();
    }

    @Override
    public boolean advance() throws PersistitException
    {
        return super.advance() && !pastEnd();
    }

    public void setRange(ValueSource lo, ValueSource hi)
    {
        boolean loNull = lo.isNull();
        boolean hiNull = hi.isNull();
        assert !(loNull && hiNull);
        boolean bothNonNull = !loNull && !hiNull;
        loSource = lo;
        hiSource = hi;
        fieldType = loNull ? hiSource.getConversionType() : loSource.getConversionType();
        loEQHi =
            bothNonNull
            ? Expressions.compare(Expressions.valueSource(loSource),
                                  Comparison.EQ,
                                  Expressions.valueSource(hiSource))
            : null;
        endComparison = null;
    }

    public void setRangeLimits(boolean loInclusive, boolean hiInclusive, boolean inequalityOK)
    {
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
        if (!inequalityOK && loEQHi != null && !loEQHi.evaluation().eval().getBool()) {
            throw new IllegalArgumentException();
        }
    }

    public MixedOrderScanStateBounded(SortCursorMixedOrder cursor,
                                      int field,
                                      boolean ascending,
                                      boolean lastBoundedColumn)
        throws PersistitException
    {
        super(cursor, field, ascending);
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(cursor.exchange.getKey());
        this.keySource = new PersistitKeyValueSource();
        this.lastBoundedColumn = lastBoundedColumn;
    }

    private void setupEndComparison(Comparison comparison, ValueSource bound)
    {
        if (endComparison == null) {
            keySource.attach(cursor.exchange.getKey(), -1, fieldType); // depth unimportant, will be set later
            endComparison =
                Expressions.compare(Expressions.valueSource(keySource),
                                    comparison,
                                    Expressions.valueSource(bound));
        }
    }

    private boolean pastEnd()
    {
        boolean pastEnd;
        if (endComparison == null) {
            pastEnd = false;
        } else {
            // hiComparisonExpression depends on exchange's key, but we need to compare the correct key segment.
            Key key = cursor.exchange.getKey();
            int keySize = key.getEncodedSize();
            keySource.attach(key, field, fieldType);
            if (keySource.isNull()) {
                pastEnd = !ascending;
            } else {
                ExpressionEvaluation evaluation = endComparison.evaluation();
                pastEnd = !evaluation.eval().getBool();
                key.setEncodedSize(keySize);
            }
        }
        return pastEnd;
    }

    private final PersistitKeyValueTarget keyTarget;
    private final PersistitKeyValueSource keySource;
    private final boolean lastBoundedColumn;
    private ValueSource loSource;
    private ValueSource hiSource;
    private boolean loInclusive;
    private boolean hiInclusive;
    private Expression endComparison;
    private Expression loEQHi;
    private AkType fieldType;
}
