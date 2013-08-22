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
import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.std.Comparison;
import com.foundationdb.server.expression.std.Expressions;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.server.types.util.ValueSources;
import com.foundationdb.server.types3.TInstance;
import com.persistit.Key;

public class OldExpressionsSortKeyAdapter extends SortKeyAdapter<ValueSource, Expression> {

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
    public void checkConstraints(BoundExpressions loExpressions,
                                 BoundExpressions hiExpressions,
                                 int f,
                                 AkCollator[] collators,
                                 TInstance[] tInstances) {
        ValueSource loValueSource = loExpressions.eval(f);
        ValueSource hiValueSource = hiExpressions.eval(f);
        if (loValueSource.isNull() && hiValueSource.isNull()) {
            // OK, they're equal
        } else if (loValueSource.isNull() || hiValueSource.isNull()) {
            throw new IllegalArgumentException(String.format("lo: %s, hi: %s", loValueSource, hiValueSource));
        } else {
            Expression loEQHi =
                    Expressions.collate(Expressions.valueSource(loValueSource),
                                        Comparison.EQ,
                                        Expressions.valueSource(hiValueSource),
                                        collators[f]);
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
    public SortKeyTarget<ValueSource> createTarget() {
        return new OldExpressionsSortKeyTarget();
    }

    @Override
    public long compare(TInstance tInstance, ValueSource one, ValueSource two) {
        return ValueSources.compare(one, two);
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
    public Expression createComparison(TInstance tInstance,
                                       AkCollator collator,
                                       ValueSource one,
                                       Comparison comparison,
                                       ValueSource two) {
        return Expressions.collate(Expressions.valueSource(one),
                                   comparison,
                                   Expressions.valueSource(two),
                                   collator);
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
    public void setOrderingMetadata(API.Ordering ordering, int index,
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
            target.append(source, akType, collator);
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
