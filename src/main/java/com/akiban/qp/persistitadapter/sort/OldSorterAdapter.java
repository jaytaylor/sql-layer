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

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.PersistitValueValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.persistit.Value;

final class OldSorterAdapter extends SorterAdapter<ValueSource, Expression, ExpressionEvaluation> {
    @Override
    protected void appendDummy(API.Ordering ordering) {
        ordering.append(DUMMY_EXPRESSION, null, ordering.ascending(0));
    }

    @Override
    protected TInstance[] tinstances(int size) {
        return null;
    }

    @Override
    protected AkType[] aktypes(int size) {
        return new AkType[size];
    }

    @Override
    protected void initTypes(RowType rowType, AkType[] ofFieldTypes, TInstance[] tFieldTypes, int i) {
        ofFieldTypes[i] = rowType.typeAt(i);
    }

    @Override
    protected void initTypes(API.Ordering ordering, int i, AkType[] akTypes, TInstance[] tInstances) {
        akTypes[i] = ordering.type(i);
    }

    @Override
    protected ExpressionEvaluation evaluation(API.Ordering ordering, QueryContext context, int i) {
        ExpressionEvaluation evaluation = ordering.expression(i).evaluation();
        evaluation.of(context);
        return evaluation;
    }

    @Override
    protected ValueSource evaluateRow(ExpressionEvaluation evaluation, Row row) {
        evaluation.of(row);
        return evaluation.eval();
    }

    @Override
    protected PersistitValueSourceAdapater createValueAdapter() {
        return new InternalAdapter();
    }

    @Override
    protected void attachValueTarget(Value value) {
        valueTarget.attach(value);
    }

    @Override
    protected void putFieldToTarget(ValueSource value, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes) {
        valueTarget.expectingType(oFieldTypes[i]);
        Converters.convert(value, valueTarget);
    }

    OldSorterAdapter() {
        super(OldExpressionsSortKeyAdapter.INSTANCE);
    }
    
    private class InternalAdapter implements PersistitValueSourceAdapater {
        @Override
        public void attach(Value value) {
            valueSource.attach(value);
        }

        @Override
        public void putToHolders(ValuesHolderRow row, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes) {
            ValueHolder valueHolder = row.holderAt(i);
            valueSource.expectedType(oFieldTypes[i]);
            valueHolder.copyFrom(valueSource);
        }

        private final PersistitValueValueSource valueSource = new PersistitValueValueSource();
    }
    
    private final PersistitValueValueTarget valueTarget =
            new PersistitValueValueTarget();

    private static final Expression DUMMY_EXPRESSION = LiteralExpression.forNull();
}
