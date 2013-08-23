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

package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.expression.BoundExpressions;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.PersistitKeyPValueSource;
import com.foundationdb.server.PersistitKeyPValueTarget;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.expression.std.Comparison;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.texpressions.TComparisonExpression;
import com.foundationdb.server.types3.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;
import com.foundationdb.server.types3.texpressions.TPreparedLiteral;
import com.persistit.Key;

public class PValueSortKeyAdapter extends SortKeyAdapter<PValueSource, TPreparedExpression> {
    
    private final static int SORT_KEY_MAX_SEGMENT_SIZE = 256;

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
    public void setColumnMetadata(Column column, int f, TInstance[] tInstances) {
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
        public void append(PValueSource source, int f, TInstance[] tInstances)
        {
            append(source, tInstances[f]);
        }

        @Override
        public void append(PValueSource source, TInstance tInstance) {
            tInstance.writeCollating(source, target);
        }

        protected final PersistitKeyPValueTarget target = new PersistitKeyPValueTarget(SORT_KEY_MAX_SEGMENT_SIZE);
    }
    
    private static class PValueSortKeySource implements SortKeySource<PValueSource> {
        @Override
        public void attach(Key key, int i, TInstance tInstance) {
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
