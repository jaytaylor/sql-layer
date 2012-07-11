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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.RankExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import static com.akiban.qp.persistitadapter.sort.SortCursor.SORT_TRAVERSE;

class MixedOrderScanStateSingleSegment extends MixedOrderScanState
{
    @Override
    public boolean startScan() throws PersistitException
    {
        Key.Direction direction = bounded() ? startBoundedScan() : startUnboundedScan();
        return cursor.exchange.traverse(direction, false) && !pastEnd();
    }

    @Override
    public boolean advance() throws PersistitException
    {
        return super.advance() && !pastEnd();
    }

    @Override
    public boolean jump(ValueSource fieldValue) throws PersistitException
    {
        boolean more;
        if (singleValue) {
            // We already know that lo = hi.
            more =
                Expressions.compare(Expressions.valueSource(fieldValue),
                                    Comparison.EQ,
                                    Expressions.valueSource(loSource)).evaluation().eval().getBool();
        } else if (bounded()) {
            long compareLo =
                new RankExpression(Expressions.valueSource(fieldValue),
                                   Expressions.valueSource(loSource)).evaluation().eval().getInt();
            long compareHi =
                new RankExpression(Expressions.valueSource(fieldValue),
                                   Expressions.valueSource(hiSource)).evaluation().eval().getInt();
            more =
                (loInclusive ? compareLo >= 0 : compareLo > 0) &&
                (hiInclusive ? compareHi <= 0 : compareHi < 0);

        } else {
            more = true;
        }
        if (more) {
            keyTarget.expectingType(fieldValue.getConversionType(), collator);
            Converters.convert(fieldValue, keyTarget);
            more = cursor.exchange.traverse(ascending ? Key.Direction.GTEQ : Key.Direction.LTEQ, false) && !pastEnd();
            if (!more) {
                // Go back to a key prefix known to exist.
                cursor.exchange.getKey().cut();
                // Go to the beginning or end of the range of keys, depending on direction.
                // Want to do a deep traverse here, not shallow as previously.
                cursor.exchange.append(ascending ? Key.BEFORE : Key.AFTER);
                boolean resume = cursor.exchange.traverse(ascending ? Key.Direction.GTEQ : Key.Direction.LTEQ, true);
                assert resume : cursor.exchange;
            }
        }
        return more;
    }

    public MixedOrderScanStateSingleSegment(SortCursorMixedOrder cursor,
                                            int field,
                                            ValueSource lo,
                                            boolean loInclusive,
                                            ValueSource hi,
                                            boolean hiInclusive,
                                            boolean singleValue,
                                            boolean ascending)
        throws PersistitException
    {
        super(cursor, field, ascending);
        assert lo != null;
        assert hi != null;
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(cursor.exchange.getKey());
        this.keySource = new PersistitKeyValueSource();
        boolean loNull = lo.isNull();
        boolean hiNull = hi.isNull();
        assert !(loNull && hiNull);
        this.loSource = lo;
        this.hiSource = hi;
        this.fieldType = loNull ? hiSource.getConversionType() : loSource.getConversionType();
        this.endComparison = null;
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
        this.singleValue = singleValue;
        this.collator = cursor.collators()[field];
        if (singleValue) {
            assert !loNull;
            assert !hiNull;
            Expression loEQHi = Expressions.compare(Expressions.valueSource(loSource),
                                                    Comparison.EQ,
                                                    Expressions.valueSource(hiSource));
            if (!loEQHi.evaluation().eval().getBool()) {
                throw new IllegalArgumentException();
            }
        }
    }

    public MixedOrderScanStateSingleSegment(SortCursorMixedOrder cursor, int field)
        throws PersistitException
    {
        super(cursor, field, cursor.ordering().ascending(field));
        this.keyTarget = new PersistitKeyValueTarget();
        this.keyTarget.attach(cursor.exchange.getKey());
        this.keySource = new PersistitKeyValueSource();
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

    private Key.Direction startUnboundedScan() throws PersistitException
    {
        Key.Direction direction;
        if (ascending) {
            cursor.exchange.append(Key.BEFORE);
            direction = Key.GT;
        } else {
            cursor.exchange.append(Key.AFTER);
            direction = Key.LT;
        }
        SORT_TRAVERSE.hit();
        return direction;
    }

    private Key.Direction startBoundedScan() throws PersistitException
    {
        // About null handling: See comment in SortCursorUnidirectional.evaluateBoundaries.
        Key.Direction direction;
        if (ascending) {
            if (loSource.isNull()) {
                cursor.exchange.append(null);
                direction = Key.GT;
            } else {
                keyTarget.expectingType(loSource.getConversionType(), collator);
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
                keyTarget.expectingType(hiSource.getConversionType(), collator);
                Converters.convert(hiSource, keyTarget);
                direction = hiInclusive ? Key.LTEQ : Key.LT;
            }
            if (!loSource.isNull()) {
                setupEndComparison(loInclusive ? Comparison.GE : Comparison.GT, loSource);
            }
        }
        SORT_TRAVERSE.hit();
        return direction;
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

    private boolean bounded()
    {
        return loSource != null && hiSource != null;
    }

    private final PersistitKeyValueTarget keyTarget;
    private final PersistitKeyValueSource keySource;
    private ValueSource loSource;
    private ValueSource hiSource;
    private boolean loInclusive;
    private boolean hiInclusive;
    private Expression endComparison;
    private AkType fieldType;
    private AkCollator collator;
    // singleValue is true if this scan state represents a key segment constrained to be a single value,
    // singleValue is false otherwise. This can only happen in the last bound of an index scan. E.g.
    // if we have an index on (a, b), and the index scan is (a = 1, 0 < b < 10), then singleValue is
    // true for a, false for b.
    private boolean singleValue = false;
}
