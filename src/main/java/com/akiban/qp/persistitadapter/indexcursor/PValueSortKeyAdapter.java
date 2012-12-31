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

import com.akiban.ais.model.Column;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.PersistitKeyPValueSource;
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

public class PValueSortKeyAdapter extends SortKeyAdapter<PValueSource, TPreparedExpression> {

    private PValueSortKeyAdapter() {}
    
    public static final SortKeyAdapter<PValueSource, TPreparedExpression> INSTANCE = new PValueSortKeyAdapter();
    
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
    public void checkConstraints(BoundExpressions loExpressions,
                                 BoundExpressions hiExpressions,
                                 int f,
                                 AkCollator[] collators,
                                 TInstance[] tInstances) {
        PValueSource loValueSource = loExpressions.pvalue(f);
        PValueSource hiValueSource = hiExpressions.pvalue(f);
        if (loValueSource.isNull() && hiValueSource.isNull()) {
            // OK, they're equal
        } else if (loValueSource.isNull() || hiValueSource.isNull()) {
            throw new IllegalArgumentException(String.format("lo: %s, hi: %s", loValueSource, hiValueSource));
        } else {
            TInstance tInstance = tInstances[f];
            TPreparedExpression loEQHi =
                    new TComparisonExpression(
                            new TPreparedLiteral(tInstance, loValueSource),
                            Comparison.EQ,
                            new TPreparedLiteral(tInstance, hiValueSource)
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
    public SortKeyTarget<PValueSource> createTarget() {
        return new PValueSortKeyTarget();
    }

    @Override
    public boolean isNull(PValueSource source) {
        return source.isNull();
    }

    @Override
    public SortKeySource<PValueSource> createSource(TInstance tInstance) {
        return new PValueSortKeySource(tInstance);
    }

    @Override
    public long compare(TInstance tInstance, PValueSource one, PValueSource two) {
        return TClass.compare(tInstance, one, tInstance, two);
    }

    @Override
    public TPreparedExpression createComparison(TInstance tInstance,
                                                AkCollator collator,
                                                PValueSource one,
                                                Comparison comparison,
                                                PValueSource two) {
        TPreparedExpression arg1 = new TPreparedLiteral(tInstance, one);
        TPreparedExpression arg2 = new TPreparedLiteral(tInstance, two);
        return new TComparisonExpression(arg1, comparison, arg2);
    }

    @Override
    public boolean evaluateComparison(TPreparedExpression comparison, QueryContext queryContext) {
        TEvaluatableExpression eval = comparison.build();
        eval.evaluate();
        return eval.resultValue().getBoolean();
    }

    @Override
    public PValueSource eval(Row row, int field) {
        return row.pvalue(field);
    }

    @Override
    public void setOrderingMetadata(API.Ordering ordering, int index,
                                    TInstance[] tInstances) {
        tInstances[index] = ordering.tInstance(index);
    }

    private static class PValueSortKeyTarget implements SortKeyTarget<PValueSource> {
        @Override
        public void attach(Key key) {
            target.attach(key);
        }

        @Override
        public void append(PValueSource source, int f, AkType[] akTypes, TInstance[] tInstances,
                           AkCollator[] collators)
        {
            append(source, null, tInstances[f], null);
        }

        @Override
        public void append(PValueSource source, AkType akType, TInstance tInstance, AkCollator collator) {
            tInstance.writeCollating(source, target);
        }

        @Override
        public void append(PValueSource source, AkCollator collator, TInstance tInstance) {
            append(source, null, tInstance, collator);
        }

        protected final PersistitKeyPValueTarget target = new PersistitKeyPValueTarget();
    }
    
    private static class PValueSortKeySource implements SortKeySource<PValueSource> {
        @Override
        public void attach(Key key, int i, AkType fieldType, TInstance tInstance) {
            source.attach(key, i, tInstance);
        }

        @Override
        public PValueSource asSource() {
            return source;
        }
        
        public PValueSortKeySource(TInstance tInstance) {
            source = new PersistitKeyPValueSource(tInstance);
        }
        
        private final PersistitKeyPValueSource source;
    }
}
