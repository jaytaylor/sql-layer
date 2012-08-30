/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.persistitadapter.indexcursor;


import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import static com.akiban.qp.persistitadapter.indexcursor.IndexCursor.INDEX_TRAVERSE;

class MixedOrderScanStateSingleSegment<S,E> extends MixedOrderScanState<S>
{
    @Override
    public boolean startScan() throws PersistitException
    {
        Key.Direction direction = bounded() ? startBoundedScan() : startUnboundedScan();
        return cursor.exchange().traverse(direction, false) && !pastEnd();
    }

    @Override
    public boolean advance() throws PersistitException
    {
        return super.advance() && !pastEnd();
    }

    @Override
    public boolean jump(S fieldValue) throws PersistitException
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
            Exchange exchange = cursor.exchange();
            more = exchange.traverse(ascending ? Key.Direction.GTEQ : Key.Direction.LTEQ, false) && !pastEnd();
            if (!more) {
                // Go back to a key prefix known to exist.
                exchange.getKey().cut();
                // Go to the beginning or end of the range of keys, depending on direction.
                // Want to do a deep traverse here, not shallow as previously.
                exchange.append(ascending ? Key.BEFORE : Key.AFTER);
                boolean resume = exchange.traverse(ascending ? Key.Direction.GTEQ : Key.Direction.LTEQ, true);
                assert resume : exchange;
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
        throws PersistitException
    {
        super(cursor, field, ascending);
        assert lo != null;
        assert hi != null;
        this.fieldType = cursor.akTypeAt(field);
        this.collator = cursor.collatorAt(field);
        this.fieldTInstance = cursor.tInstanceAt(field);
        this.keyTarget = sortKeyAdapter.createTarget();
        this.keyTarget.attach(cursor.exchange().getKey());
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

    public MixedOrderScanStateSingleSegment(IndexCursorMixedOrder cursor,
                                            int field,
                                            boolean ascending,
                                            SortKeyAdapter<S, E> sortKeyAdapter)
        throws PersistitException
    {
        super(cursor, field, ascending);
        this.keyTarget = sortKeyAdapter.createTarget();
        this.keyTarget.attach(cursor.exchange().getKey());
        this.keySource = sortKeyAdapter.createSource(cursor.tInstanceAt(field));
        this.sortKeyAdapter = sortKeyAdapter;
    }

    private void setupEndComparison(Comparison comparison, S bound)
    {
        if (endComparison == null) {
            keySource.attach(cursor.exchange().getKey(), -1, fieldType, fieldTInstance); // depth unimportant, will be set later
            endComparison = sortKeyAdapter.createComparison(fieldTInstance, collator, keySource.asSource(), comparison, bound);
        }
    }

    private Key.Direction startUnboundedScan() throws PersistitException
    {
        Key.Direction direction;
        if (ascending) {
            cursor.exchange().append(Key.BEFORE);
            direction = Key.GT;
        } else {
            cursor.exchange().append(Key.AFTER);
            direction = Key.LT;
        }
        INDEX_TRAVERSE.hit();
        return direction;
    }

    private Key.Direction startBoundedScan() throws PersistitException
    {
        // About null handling: See comment in IndexCursorUnidirectional.evaluateBoundaries.
        Key.Direction direction;
        if (ascending) {
            if (sortKeyAdapter.isNull(loSource)) {
                cursor.exchange().append(null);
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
                    cursor.exchange().append(null);
                } else {
                    cursor.exchange().append(Key.AFTER);
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
            Key key = cursor.exchange().getKey();
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
