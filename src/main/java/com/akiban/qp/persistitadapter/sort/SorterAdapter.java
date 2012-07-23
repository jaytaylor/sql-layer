/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter.sort;

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
            
//            this.ordering.append(DUMMY_EXPRESSION, DUMMY_T_EXPRESSION, ordering.ascending(0));
            appendDummy(ordering);
        }
        
        int nsort = ordering.sortColumns();
        this.evaluations = new ArrayList<V>(nsort);
//        this.orderingTypes = new AkType[nsort];
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

    protected abstract PersistitValueAdapater createValueAdapter();
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

    public interface PersistitValueAdapater {
        void attach(Value value);
        void putToHolders(ValuesHolderRow row, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes);
    }
}
