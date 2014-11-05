/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.foundationdb.qp.storeadapter.indexcursor;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TInstance;
import com.persistit.Key;
import com.persistit.Value;

import java.util.ArrayList;
import java.util.List;

abstract class SorterAdapter<S,E,V> {
    
    protected SorterAdapter(SortKeyAdapter<S,E> sortKeyAdapter) {
        this.sortKeyAdapter = sortKeyAdapter;
        keyTarget = sortKeyAdapter.createTarget("sort");
    }
    
    public void init(RowType rowType, Ordering ordering, Key key, Value value, QueryContext context, QueryBindings bindings,
                     API.SortOption sortOption)
    {
        
        this.keyTarget.attach(key);
        
        int rowFields  = rowType.nFields();
        this.tFieldTypes = tinstances(rowFields);
        for (int i = 0; i < rowFields; i++) {
            initTypes(rowType, tFieldTypes, i);
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
        this.evaluations = new ArrayList<>(nsort);
        this.tOrderingTypes = tinstances(nsort);
        for (int i = 0; i < nsort; i++) {
            initTypes(ordering, i, tOrderingTypes);
            V evaluation = evaluation(ordering, context, bindings, i);
            evaluations.add(evaluation);
        }
    }

    protected abstract void appendDummy(Ordering ordering);

    protected abstract TInstance[] tinstances(int size);

    public void evaluateToKey(Row row, int i) {
        V evaluation = evaluations.get(i);
        S keySource = evaluateRow(evaluation, row);
        keyTarget.append(keySource, i, tOrderingTypes);
    }

    public TInstance[] tFieldTypes() {
        return tFieldTypes;
    }

    public boolean preserveDuplicates() {
        return preserveDuplicates;
    }
    
    protected abstract void initTypes(RowType rowType, TInstance[] tFieldTypes, int i);
    protected abstract void initTypes(Ordering ordering, int i, TInstance[] tInstances);
    protected abstract V evaluation(Ordering ordering, QueryContext context, QueryBindings bindings, int i);
    protected abstract S evaluateRow(V evaluation, Row row);
    protected abstract void attachValueTarget(Value value);

    protected abstract PersistitValueSourceAdapter createValueAdapter();
    private final SortKeyAdapter<S,E> sortKeyAdapter;

    private final SortKeyTarget<S> keyTarget;
    private boolean preserveDuplicates;
    //private AkCollator orderingCollators[];
    private TInstance tFieldTypes[], tOrderingTypes[];

    private List<V> evaluations;

    public void evaluateToTarget(Row row, int i) {
        S field = sortKeyAdapter.get(row, i);
        putFieldToTarget(field, i, tFieldTypes);
    }

    protected abstract void putFieldToTarget(S value, int i, TInstance[] tFieldTypes);

    public interface PersistitValueSourceAdapter {
        void attach(Value value);
        void putToHolders(ValuesHolderRow row, int i, TInstance[] fieldTypes);
    }
}
