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

package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.expression.std.Comparison;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types3.TInstance;
import com.persistit.Key;

import static com.foundationdb.qp.persistitadapter.indexcursor.IndexCursor.INDEX_TRAVERSE;

class MixedOrderScanStateSingleSegment<S,E> extends MixedOrderScanState<S>
{
    @Override
    public boolean startScan()
    {
        Key.Direction direction = bounded() ? startBoundedScan() : startUnboundedScan();
        return cursor.traverse(direction, false) && !pastEnd();
    }

    @Override
    public boolean advance()
    {
        return super.advance() && !pastEnd();
    }

    @Override
    public boolean jump(S fieldValue)
    {
        boolean more;
        if (singleValue) {
            // We already know that lo = hi.
            more = sortKeyAdapter.areEqual(fieldTInstance, collator, fieldValue, loSource, cursor.context);
        } else if (bounded()) {
            long compareLo = sortKeyAdapter.compare(fieldTInstance, fieldValue,  loSource);
            long compareHi = sortKeyAdapter.compare(fieldTInstance, fieldValue, hiSource);
            more =
                (loInclusive ? compareLo >= 0 : compareLo > 0) &&
                (hiInclusive ? compareHi <= 0 : compareHi < 0);

        } else {
            more = true;
        }
        if (more) {
            keyTarget.append(fieldValue, collator, fieldTInstance);
            more = cursor.traverse(ascending ? Key.Direction.GTEQ : Key.Direction.LTEQ, false) && !pastEnd();
            if (!more) {
                // Go back to a key prefix known to exist.
                cursor.key().cut();
                // Go to the beginning or end of the range of keys, depending on direction.
                // Want to do a deep traverse here, not shallow as previously.
                cursor.key().append(ascending ? Key.BEFORE : Key.AFTER);
                boolean resume = cursor.traverse(ascending ? Key.Direction.GTEQ : Key.Direction.LTEQ, true);
                assert resume : cursor;
            }
        }
        return more;
    }

    public MixedOrderScanStateSingleSegment(IndexCursorMixedOrder cursor,
                                            int field,
                                            S lo,
                                            boolean loInclusive,
                                            S hi,
                                            boolean hiInclusive,
                                            boolean singleValue,
                                            boolean ascending,
                                            SortKeyAdapter<S, E> sortKeyAdapter)
    {
        super(cursor, field, ascending);
        assert lo != null;
        assert hi != null;
        this.fieldType = cursor.akTypeAt(field);
        this.collator = cursor.collatorAt(field);
        this.fieldTInstance = cursor.tInstanceAt(field);
        this.keyTarget = sortKeyAdapter.createTarget();
        this.keyTarget.attach(cursor.key());
        this.keySource = sortKeyAdapter.createSource(fieldTInstance);
        this.sortKeyAdapter = sortKeyAdapter;
        boolean loNull = sortKeyAdapter.isNull(lo);
        boolean hiNull = sortKeyAdapter.isNull(hi);
        assert !(loNull && hiNull);
        this.loSource = lo;
        this.hiSource = hi;
        this.endComparison = null;
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
        this.singleValue = singleValue;
        if (singleValue) {
            assert !loNull;
            assert !hiNull;
            boolean loEQHi = sortKeyAdapter.areEqual(fieldTInstance, collator, loSource, hiSource, cursor.context);
            if (!loEQHi) {
                throw new IllegalArgumentException();
            }
        }
    }

    protected MixedOrderScanStateSingleSegment(IndexCursorMixedOrder cursor,
                                            int field,
                                            boolean ascending,
                                            SortKeyAdapter<S, E> sortKeyAdapter,
                                            TInstance tInstance)
    {
        super(cursor, field, ascending);
        this.keyTarget = sortKeyAdapter.createTarget();
        this.keyTarget.attach(cursor.key());
        this.keySource = sortKeyAdapter.createSource(tInstance);
        this.sortKeyAdapter = sortKeyAdapter;
        this.fieldTInstance = tInstance;
    }

    public MixedOrderScanStateSingleSegment(IndexCursorMixedOrder cursor,
                                            int field,
                                            boolean ascending,
                                            SortKeyAdapter<S, E> sortKeyAdapter)
    {
        this(cursor, field, ascending, sortKeyAdapter, cursor.tInstanceAt(field));
    }

    private void setupEndComparison(Comparison comparison, S bound)
    {
        if (endComparison == null) {
            keySource.attach(cursor.key(), -1, fieldType, fieldTInstance); // depth unimportant, will be set later
            endComparison = sortKeyAdapter.createComparison(fieldTInstance, collator, keySource.asSource(), comparison, bound);
        }
    }

    private Key.Direction startUnboundedScan()
    {
        Key.Direction direction;
        if (ascending) {
            cursor.key().append(Key.BEFORE);
            direction = Key.GT;
        } else {
            cursor.key().append(Key.AFTER);
            direction = Key.LT;
        }
        INDEX_TRAVERSE.hit();
        return direction;
    }

    private Key.Direction startBoundedScan()
    {
        // About null handling: See comment in IndexCursorUnidirectional.evaluateBoundaries.
        Key.Direction direction;
        if (ascending) {
            if (sortKeyAdapter.isNull(loSource)) {
                cursor.key().append(null);
                direction = Key.GT;
            } else {
                keyTarget.append(loSource, fieldType, fieldTInstance, collator);
                direction = loInclusive ? Key.GTEQ : Key.GT;
            }
            if (!sortKeyAdapter.isNull(hiSource)) {
                setupEndComparison(hiInclusive ? Comparison.LE : Comparison.LT, hiSource);
            }
            // else: endComparison stays null, which causes pastEnd() to always return false.
        } else {
            if (sortKeyAdapter.isNull(hiSource)) {
                if (sortKeyAdapter.isNull(loSource)) {
                    cursor.key().append(null);
                } else {
                    cursor.key().append(Key.AFTER);
                }
                direction = Key.LT;
            } else {
                keyTarget.append(hiSource, fieldType, fieldTInstance, collator);
                direction = hiInclusive ? Key.LTEQ : Key.LT;
            }
            if (!sortKeyAdapter.isNull(loSource)) {
                setupEndComparison(loInclusive ? Comparison.GE : Comparison.GT, loSource);
            }
        }
        INDEX_TRAVERSE.hit();
        return direction;
    }

    private boolean pastEnd()
    {
        boolean pastEnd;
        if (endComparison == null) {
            pastEnd = false;
        } else {
            // hiComparisonExpression depends on exchange's key, but we need to compare the correct key segment.
            Key key = cursor.key();
            int keySize = key.getEncodedSize();
            keySource.attach(key, field, fieldType, fieldTInstance);
            if (sortKeyAdapter.isNull(keySource.asSource())) {
                pastEnd = !ascending;
            } else {
                pastEnd = !sortKeyAdapter.evaluateComparison(endComparison, cursor.context);
                key.setEncodedSize(keySize);
            }
        }
        return pastEnd;
    }

    private boolean bounded()
    {
        return loSource != null && hiSource != null;
    }

    private final SortKeyAdapter<S, E> sortKeyAdapter;
    private final SortKeyTarget<S> keyTarget;
    private final SortKeySource<S> keySource;
    private S loSource;
    private S hiSource;
    private boolean loInclusive;
    private boolean hiInclusive;
    private E endComparison;
    private AkType fieldType;
    private AkCollator collator;
    private TInstance fieldTInstance;
    // singleValue is true if this scan state represents a key segment constrained to be a single value,
    // singleValue is false otherwise. This can only happen in the last bound of an index scan. E.g.
    // if we have an index on (a, b), and the index scan is (a = 1, 0 < b < 10), then singleValue is
    // true for a, false for b.
    private boolean singleValue = false;
}
