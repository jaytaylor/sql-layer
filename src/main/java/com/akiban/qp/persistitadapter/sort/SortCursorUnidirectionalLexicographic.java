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

import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;

// For a lexicographic (mysqlish) index scan

class SortCursorUnidirectionalLexicographic extends SortCursorUnidirectional
{
    // SortCursorUnidirectional interface

    public static SortCursorUnidirectionalLexicographic create(QueryContext context,
                                                               IterationHelper iterationHelper,
                                                               IndexKeyRange keyRange,
                                                               API.Ordering ordering)
    {
        return new SortCursorUnidirectionalLexicographic(context, iterationHelper, keyRange, ordering);
    }

    // For use by this class

    private SortCursorUnidirectionalLexicographic(QueryContext context,
                                                  IterationHelper iterationHelper,
                                                  IndexKeyRange keyRange,
                                                  API.Ordering ordering)
    {
        super(context, iterationHelper, keyRange, ordering);
    }

    @Override
    protected <S> void evaluateBoundaries(QueryContext context, SortStrategy<S> strategy)
    {
        BoundExpressions startExpressions = null;
        if (startBoundColumns == 0 || start == null) {
            startKey.append(startBoundary);
        } else {
            startExpressions = start.boundExpressions(context);
            startKey.clear();
            strategy.attachToStartKey(startKey);
            for (int f = 0; f < startBoundColumns; f++) {
                if (start.columnSelector().includesColumn(f)) {
                    S source = strategy.get(startExpressions, f);
                    strategy.appendToStartKey(source,types[f]);
                }
            }
        }
        BoundExpressions endExpressions;
        if (endBoundColumns == 0 || end == null) {
            endKey = null;
        } else {
            endExpressions = end.boundExpressions(context);
            endKey.clear();
            strategy.attachToEndKey(endKey);
            for (int f = 0; f < endBoundColumns; f++) {
                if (end.columnSelector().includesColumn(f)) {
                    S source = strategy.get(endExpressions, f);
                    if (strategy.isNull(source) && startExpressions != null && !startExpressions.eval(f).isNull()) {
                        endKey.append(Key.AFTER);
                    } else {
                        strategy.appendToEndKey(source, types[f]);
                    }
                } else {
                    endKey.append(Key.AFTER);
                }
            }
        }
    }
}
