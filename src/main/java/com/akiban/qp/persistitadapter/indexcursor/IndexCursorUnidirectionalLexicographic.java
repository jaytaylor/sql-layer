/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.persistit.Key;

// For a lexicographic (mysqlish) index scan

class IndexCursorUnidirectionalLexicographic<S> extends IndexCursorUnidirectional<S>
{
    // IndexCursorUnidirectional interface

    public static <S> IndexCursorUnidirectionalLexicographic<S> create(QueryContext context,
                                                               QueryBindings bindings,
                                                               IterationHelper iterationHelper,
                                                               IndexKeyRange keyRange,
                                                               API.Ordering ordering,
                                                               SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        return new IndexCursorUnidirectionalLexicographic<>(context, bindings, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    // For use by this class

    private IndexCursorUnidirectionalLexicographic(QueryContext context,
                                                   QueryBindings bindings,
                                                   IterationHelper iterationHelper,
                                                   IndexKeyRange keyRange,
                                                   API.Ordering ordering,
                                                   SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        super(context, bindings, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    @Override
    protected void evaluateBoundaries(QueryContext context, SortKeyAdapter<S, ?> keyAdapter)
    {
        BoundExpressions startExpressions = null;
        if (startBoundColumns == 0 || start == null) {
            startKey.append(startBoundary);
        } else {
            startExpressions = start.boundExpressions(context, bindings);
            clearStart();
            for (int f = 0; f < startBoundColumns; f++) {
                if (start.columnSelector().includesColumn(f)) {
                    S source = keyAdapter.get(startExpressions, f);
                    startKey.append(source, type(f), tInstance(f), collator(f));
                }
            }
        }
        BoundExpressions endExpressions;
        if (endBoundColumns == 0 || end == null) {
            endKey = null;
        } else {
            endExpressions = end.boundExpressions(context, bindings);
            clearEnd();
            for (int f = 0; f < endBoundColumns; f++) {
                if (end.columnSelector().includesColumn(f)) {
                    S source = keyAdapter.get(endExpressions, f);
                    if (keyAdapter.isNull(source) && startExpressions != null && !startExpressions.eval(f).isNull()) {
                        endKey.append(Key.AFTER);
                    } else {
                        endKey.append(source, type(f), tInstance(f), collator(f));
                    }
                } else {
                    endKey.append(Key.AFTER);
                }
            }
        }
    }
}
