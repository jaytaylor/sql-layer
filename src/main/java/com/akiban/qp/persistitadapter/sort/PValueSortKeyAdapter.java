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
import com.akiban.server.PersistitKeyPValueTarget;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TComparisonExpression;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedLiteral;
import com.persistit.Key;

public final class PValueSortKeyAdapter implements SortKeyAdapter<PValueSource> {

    @Override
    public AkType[] createAkTypes(int size) {
        return null;
    }

    @Override
    public AkCollator[] createAkCollators(int size) {
        return null;
    }

    @Override
    public TInstance[] createTInstances(int size) {
        return new TInstance[size];
    }

    @Override
    public void setColumnMetadata(Column column, int f, AkType[] akTypes, AkCollator[] collators,
                                  TInstance[] tInstances) {
        tInstances[f] = column.tInstance();
    }

    @Override
    public void checkConstraints(BoundExpressions loExpressions, BoundExpressions hiExpressions, int f) {
        PValueSource loValueSource = loExpressions.pvalue(f);
        PValueSource hiValueSource = hiExpressions.pvalue(f);
        if (loValueSource.isNull() && hiValueSource.isNull()) {
            // OK, they're equal
        } else if (loValueSource.isNull() || hiValueSource.isNull()) {
            throw new IllegalArgumentException(String.format("lo: %s, hi: %s", loValueSource, hiValueSource));
        } else {
            TPreparedExpression loEQHi =
                    new TComparisonExpression(
                            new TPreparedLiteral(null, loValueSource),
                            Comparison.EQ,
                            new TPreparedLiteral(null, hiValueSource)
                    );
            TEvaluatableExpression eval = loEQHi.build();
            eval.evaluate();
            if (!eval.resultValue().getBoolean()) {
                throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public PValueSource[] createSourceArray(int size) {
        return new PValueSource[size];
    }

    @Override
    public PValueSource get(BoundExpressions boundExpressions, int f) {
        return boundExpressions.pvalue(f);
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
    public void appendToStartKey(PValueSource source, int f, AkType[] akTypes, TInstance[] tInstances, AkCollator[] collators) {
        appendTo(startKeyTarget, source, tInstances[f]);
    }

    @Override
    public void appendToEndKey(PValueSource source, int f, AkType[] akTypes, TInstance[] tInstances, AkCollator[] collators) {
        appendTo(endKeyTarget, source, tInstances[f]);
    }

    @Override
    public boolean isNull(PValueSource source) {
        return source.isNull();
    }

    private void appendTo(PersistitKeyPValueTarget target, PValueSource source, TInstance tInstance) {
        target.expectingType(source.getUnderlyingType());
        TClass tClass = tInstance.typeClass();
        tClass.writeCollating(source, tInstance, target);
    }

    protected final PersistitKeyPValueTarget startKeyTarget = new PersistitKeyPValueTarget();
    protected final PersistitKeyPValueTarget endKeyTarget = new PersistitKeyPValueTarget();
}
