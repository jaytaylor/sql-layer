
package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.persistit.Key;

// For a lexicographic (mysqlish) index scan

class IndexCursorUnidirectionalLexicographic<S> extends IndexCursorUnidirectional<S>
{
    // IndexCursorUnidirectional interface

    public static <S> IndexCursorUnidirectionalLexicographic<S> create(QueryContext context,
                                                               IterationHelper iterationHelper,
                                                               IndexKeyRange keyRange,
                                                               API.Ordering ordering,
                                                               SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        return new IndexCursorUnidirectionalLexicographic<>(context, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    // For use by this class

    private IndexCursorUnidirectionalLexicographic(QueryContext context,
                                                   IterationHelper iterationHelper,
                                                   IndexKeyRange keyRange,
                                                   API.Ordering ordering,
                                                   SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        super(context, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    @Override
    protected void evaluateBoundaries(QueryContext context, SortKeyAdapter<S, ?> keyAdapter)
    {
        BoundExpressions startExpressions = null;
        if (startBoundColumns == 0 || start == null) {
            startKey.append(startBoundary);
        } else {
            startExpressions = start.boundExpressions(context);
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
            endExpressions = end.boundExpressions(context);
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
