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
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.persistit.Key;
import com.persistit.Value;

import java.util.ArrayList;
import java.util.List;

abstract class SorterAdapter<S,E,V> {
    
    protected SorterAdapter(SortKeyAdapter<S,E> sortKeyAdapter) {
        this.sortKeyAdapter = sortKeyAdapter;
        keyTarget = sortKeyAdapter.createTarget();
    }
    
    public void init(RowType rowType, Ordering ordering, Key key, Value value, QueryContext context,
                     API.SortOption sortOption)
    {
        
        this.keyTarget.attach(key);
        
        int rowFields  = rowType.nFields();
        this.oFieldTypes = aktypes(rowFields);
        this.tFieldTypes = tinstances(rowFields);
        for (int i = 0; i < rowFields; i++) {
            initTypes(rowType, oFieldTypes, tFieldTypes, i);
        }
        
        attachValueTarget(value);

        preserveDuplicates = sortOption == API.SortOption.PRESERVE_DUPLICATES;
        if (preserveDuplicates) {
            // Append a count field as a sort key, to ensure key uniqueness for Persisit. By setting
            // the ascending flag equal to that of some other sort field, we don't change an all-ASC or all-DESC sort
            // into a less efficient mixed-mode sort.
            appendDummy(ordering);
        }
        
        int nsort = ordering.sortColumns();
        this.evaluations = new ArrayList<V>(nsort);
        this.oOrderingTypes = aktypes(nsort);
        this.tOrderingTypes = tinstances(nsort);
        this.orderingCollators = new AkCollator[nsort];
        for (int i = 0; i < nsort; i++) {
            initTypes(ordering, i, oOrderingTypes, tOrderingTypes);
            orderingCollators[i] = ordering.collator(i);
            V evaluation = evaluation(ordering, context, i);
            evaluations.add(evaluation);
        }
    }

    protected abstract void appendDummy(Ordering ordering);

    protected abstract TInstance[] tinstances(int size);

    protected abstract AkType[] aktypes(int size);

    public void evaluateToKey(Row row, int i) {
        V evaluation = evaluations.get(i);
        S keySource = evaluateRow(evaluation, row);
        keyTarget.append(keySource, i, oOrderingTypes, tOrderingTypes, orderingCollators);
    }

    public AkType[] oFieldTypes() {
        return oFieldTypes;
    }

    public TInstance[] tFieldTypes() {
        return tFieldTypes;
    }

    public boolean preserveDuplicates() {
        return preserveDuplicates;
    }
    
    protected abstract void initTypes(RowType rowType, AkType[] ofFieldTypes, TInstance[] tFieldTypes, int i);
    protected abstract void initTypes(Ordering ordering, int i, AkType[] akTypes, TInstance[] tInstances);
    protected abstract V evaluation(Ordering ordering, QueryContext context, int i);
    protected abstract S evaluateRow(V evaluation, Row row);
    protected abstract void attachValueTarget(Value value);

    protected abstract PersistitValueSourceAdapter createValueAdapter();
    private final SortKeyAdapter<S,E> sortKeyAdapter;

    private final SortKeyTarget<S> keyTarget;
    private boolean preserveDuplicates;
    private AkCollator orderingCollators[];
    private AkType oFieldTypes[], oOrderingTypes[];
    private TInstance tFieldTypes[], tOrderingTypes[];

    private List<V> evaluations;

    public void evaluateToTarget(Row row, int i) {
        S field = sortKeyAdapter.get(row, i);
        putFieldToTarget(field, i, oFieldTypes, tFieldTypes);
    }

    protected abstract void putFieldToTarget(S value, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes);

    public interface PersistitValueSourceAdapter {
        void attach(Value value);
        void putToHolders(ValuesHolderRow row, int i, AkType[] oFieldTypes);
    }
}
