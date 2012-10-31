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

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitValuePValueSource;
import com.akiban.server.PersistitValuePValueTarget;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TNullExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.persistit.Value;

final class PValueSorterAdapter extends SorterAdapter<PValueSource, TPreparedExpression, TEvaluatableExpression> {
    @Override
    protected void appendDummy(API.Ordering ordering) {
        ordering.append(null, DUMMY_EXPRESSION, ordering.ascending(0));
    }

    @Override
    protected TInstance[] tinstances(int size) {
        return new TInstance[size];
    }

    @Override
    protected AkType[] aktypes(int size) {
        return null;
    }

    @Override
    protected void initTypes(RowType rowType, AkType[] ofFieldTypes, TInstance[] tFieldTypes, int i) {
        tFieldTypes[i] = rowType.typeInstanceAt(i);
    }

    @Override
    protected void initTypes(API.Ordering ordering, int i, AkType[] akTypes, TInstance[] tInstances) {
        tInstances[i] = ordering.tInstance(i);
    }

    @Override
    protected TEvaluatableExpression evaluation(API.Ordering ordering, QueryContext context, int i) {
        TEvaluatableExpression evaluation = ordering.tExpression(i).build();
        evaluation.with(context);
        return evaluation;
    }

    @Override
    protected PValueSource evaluateRow(TEvaluatableExpression evaluation, Row row) {
        evaluation.with(row);
        evaluation.evaluate();
        return evaluation.resultValue();
    }

    @Override
    protected void attachValueTarget(Value value) {
        valueTarget.attach(value);
    }

    @Override
    protected PersistitValueSourceAdapter createValueAdapter() {
        return new InternalPAdapter();
    }

    @Override
    protected void putFieldToTarget(PValueSource value, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes) {
        tFieldTypes[i].writeCanonical(value, valueTarget);
    }

    PValueSorterAdapter() {
        super(PValueSortKeyAdapter.INSTANCE);
    }
    
    private class InternalPAdapter implements PersistitValueSourceAdapter {

        @Override
        public void attach(Value value) {
            valueSource.attach(value);
        }

        @Override
        public void putToHolders(ValuesHolderRow row, int i, AkType[] oFieldTypes) {
            valueSource.getReady();
            PValueTargets.copyFrom(valueSource, row.pvalueAt(i));
        }

        private final PersistitValuePValueSource valueSource = new PersistitValuePValueSource();
    }
    
    private final PersistitValuePValueTarget valueTarget = new PersistitValuePValueTarget();
    
    private static final TPreparedExpression DUMMY_EXPRESSION = new TNullExpression(MNumeric.BIGINT.instance(true));
}
