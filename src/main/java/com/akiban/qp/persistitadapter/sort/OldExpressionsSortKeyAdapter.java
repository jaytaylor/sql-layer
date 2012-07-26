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
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.RankExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.TInstance;
import com.persistit.Key;

class OldExpressionsSortKeyAdapter extends SortKeyAdapter<ValueSource, Expression> {

    private OldExpressionsSortKeyAdapter() {}
    
    public static final SortKeyAdapter<ValueSource, Expression> INSTANCE = new OldExpressionsSortKeyAdapter();
    
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
    public ValueSource[] createSourceArray(int size) {
        return new ValueSource[size];
    }

    @Override
    public ValueSource get(BoundExpressions boundExpressions, int f) {
        return boundExpressions.eval(f);
    }

    @Override
    public SortKeyTarget<ValueSource> createTarget() {
        return new OldExpressionsSortKeyTarget();
    }

    @Override
    public long compare(TInstance tInstance, ValueSource one, ValueSource two) {
        return new RankExpression(Expressions.valueSource(one),
                Expressions.valueSource(two)).evaluation().eval().getInt();
    }

    @Override
    public boolean isNull(ValueSource source) {
        return source.isNull();
    }

    @Override
    public SortKeySource<ValueSource> createSource(TInstance tInstance) {
        return new OldExpressionsKeySource();
    }

    @Override
    public Expression createComparison(TInstance tInstance, ValueSource one, Comparison comparison, ValueSource two) {
        return Expressions.compare(Expressions.valueSource(one),
                comparison,
                Expressions.valueSource(two));
    }

    @Override
    public boolean evaluateComparison(Expression comparison, QueryContext queryContext) {
        return comparison.evaluation().eval().getBool();
    }

    @Override
    public ValueSource eval(Row row, int field) {
        return row.eval(field);
    }

    @Override
    public void setOrderingMetadata(int orderingIndex, API.Ordering ordering, int tInstancesOffset,
                                    TInstance[] tInstances) {
        // nothing to do
    }

    private static class OldExpressionsSortKeyTarget implements SortKeyTarget<ValueSource> {

        @Override
        public void attach(Key key) {
            target.attach(key);
        }

        @Override
        public void append(ValueSource source, int f, AkType[] akTypes, TInstance[] tInstances,
                           AkCollator[] collators) {
            AkType type = akTypes[f];
            AkCollator collator = collators[f];
            append(source, type, null, collator);
        }

        @Override
        public void append(ValueSource source, AkType akType, TInstance tInstance, AkCollator collator) {
            target.expectingType(akType, collator);
            Converters.convert(source, target);
        }

        @Override
        public void append(ValueSource source, AkCollator collator, TInstance tInstance) {
            target.expectingType(source.getConversionType(), collator);
            Converters.convert(source, target);
        }

        protected final PersistitKeyValueTarget target = new PersistitKeyValueTarget();
    }
    
    private static class OldExpressionsKeySource implements SortKeySource<ValueSource> {
        @Override
        public void attach(Key key, int i, AkType fieldType, TInstance tInstance) {
            source.attach(key, i, fieldType);
        }

        @Override
        public ValueSource asSource() {
            return source;
        }
        
        private PersistitKeyValueSource source = new PersistitKeyValueSource();
    }
}
