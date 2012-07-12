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

import com.akiban.ais.model.Column;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.TInstance;
import com.persistit.Key;

class OldExpressionsSortStrategy implements SortStrategy<ValueSource> {

    @Override
    public AkType[] createAkTypes(int size) {
        return new AkType[size];
    }

    @Override
    public AkCollator[] createAkCollators(int size) {
        return new AkCollator[size];
    }

    @Override
    public TInstance[] createTInstances(int size) {
        return null;
    }

    @Override
    public void setColumnMetadata(Column column, int f, AkType[] akTypes, AkCollator[] collators, TInstance[] tInstances) {
        akTypes[f] = column.getType().akType();
        collators[f] = column.getCollator();
    }

    @Override
    public void checkConstraints(BoundExpressions loExpressions, BoundExpressions hiExpressions, int f) {
        ValueSource loValueSource = loExpressions.eval(f);
        ValueSource hiValueSource = hiExpressions.eval(f);
        if (loValueSource.isNull() && hiValueSource.isNull()) {
            // OK, they're equal
        } else if (loValueSource.isNull() || hiValueSource.isNull()) {
            throw new IllegalArgumentException(String.format("lo: %s, hi: %s", loValueSource, hiValueSource));
        } else {
            Expression loEQHi =
                    Expressions.compare(Expressions.valueSource(loValueSource),
                            Comparison.EQ,
                            Expressions.valueSource(hiValueSource));
            if (!loEQHi.evaluation().eval().getBool()) {
                throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public ValueSource[] createSourceArray(int size) {
        return new ValueSource[size];
    }

    @Override
    public ValueSource get(BoundExpressions boundExpressions, int f) {
        return boundExpressions.eval(f);
    }

    @Override
    public void attachToStartKey(Key key) {
        startKeyTarget.attach(key);
    }

    @Override
    public void attachToEndKey(Key key) {
        endKeyTarget.attach(key);
    }

    @Override
    public void appendToStartKey(ValueSource source, int f, AkType[] akTypes, TInstance[] tInstances, AkCollator[] collators) {
        appendTo(startKeyTarget, source, akTypes[f], collators[f]);
    }

    @Override
    public void appendToEndKey(ValueSource source, int f, AkType[] akTypes, TInstance[] tInstances, AkCollator[] collators) {
        appendTo(endKeyTarget, source, akTypes[f], collators[f]);
    }

    @Override
    public boolean isNull(ValueSource source) {
        return source.isNull();
    }

    private void appendTo(PersistitKeyValueTarget target, ValueSource source, AkType type, AkCollator collator) {
        target.expectingType(type, collator);
        Converters.convert(source, target);
    }

    protected final PersistitKeyValueTarget startKeyTarget = new PersistitKeyValueTarget();
    protected final PersistitKeyValueTarget endKeyTarget = new PersistitKeyValueTarget();
}
