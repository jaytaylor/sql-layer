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

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class IndexCursorUnidirectional<S> extends IndexCursor<S>
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        evaluateBoundaries(context, sortKeyAdapter);
        initializeForOpen();
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        Row next = null;
        if (exchange != null) {
            try {
                SORT_TRAVERSE.hit();
                if (exchange.traverse(keyComparison, true)) {
                    next = row();
                    // If we're scanning a unique key index, then the row format has the declared key in the
                    // Persistit key, and undeclared hkey columns in the Persistit value. An index scan may actually
                    // restrict the entire declared key and leading hkeys fields. If this happens, then the first
                    // row found by exchange.traverse may actually not qualify -- those values may be lower than
                    // startKey. This can happen at most once per scan. pastStart indicates whether we have gotten
                    // past the startKey.
                    if (!pastStart && beforeStart(next)) {
                        next = null;
                        if (exchange.traverse(subsequentKeyComparison, true)) {
                            next = row();
                            pastStart = true;
                        } else {
                            close();
                        }
                    }
                    if (next != null && pastEnd(next)) {
                        next = null;
                        close();
                    }
                } else {
                    close();
                }
            } catch (PersistitException e) {
                close();
                adapter.handlePersistitException(e);
            }
        }
        keyComparison = subsequentKeyComparison;
        return next;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        assert keyRange != null;
        keyRange =
            direction == FORWARD
            ? keyRange.resetLo(new IndexBound(row, columnSelector))
            : keyRange.resetHi(new IndexBound(row, columnSelector));
        initializeCursor(keyRange, ordering);
        reevaluateBoundaries(context, sortKeyAdapter);
        initializeForOpen();
    }

    // IndexCursorUnidirectional interface

    public static <S> IndexCursorUnidirectional<S> create(QueryContext context,
                                                          IterationHelper iterationHelper,
                                                          IndexKeyRange keyRange,
                                                          API.Ordering ordering,
                                                          SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        return
            keyRange == null // occurs if we're doing a Sort_Tree
            ? new IndexCursorUnidirectional<S>(context, iterationHelper, ordering, sortKeyAdapter)
            : new IndexCursorUnidirectional<S>(context, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    // For use by this subclasses

    protected IndexCursorUnidirectional(QueryContext context,
                                        IterationHelper iterationHelper,
                                        IndexKeyRange keyRange,
                                        API.Ordering ordering,
                                        SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        super(context, iterationHelper, sortKeyAdapter, keyRange);
        // end state never changes. start state can change on a jump, so it is set in initializeCursor.
        this.endBoundColumns = keyRange.boundColumns();
        this.endKey = endBoundColumns == 0 ? null : PersistitIndexRow.newIndexRow(adapter, keyRange.indexRowType());
        this.sortKeyAdapter = sortKeyAdapter;
        initializeCursor(keyRange, ordering);
    }

    protected boolean beforeStart(Row row)
    {
        boolean beforeStart;
        if (startKey == null) {
            beforeStart = false;
        } else {
            PersistitIndexRow current = (PersistitIndexRow) row;
            int c = current.compareTo(startKey) * direction;
            beforeStart = c < 0 || c == 0 && !startInclusive;
        }
        return beforeStart;
    }

    protected boolean pastEnd(Row row)
    {
        boolean pastEnd;
        if (endKey == null) {
            pastEnd = false;
        } else {
            PersistitIndexRow current = (PersistitIndexRow) row;
            int c = current.compareTo(endKey) * direction;
            pastEnd = c > 0 || c == 0 && !endInclusive;
        }
        return pastEnd;
    }

    // For use by this class

    private void initializeForOpen()
    {
        exchange.clear();
        if (startKey != null) {
            // boundColumns > 0 means that startKey has some values other than BEFORE or AFTER. start == null
            // could happen in a lexicographic scan, and indicates no lower bound (so we're starting at BEFORE or AFTER).
            if ((startBoundColumns > 0 && start != null) &&
                (direction == FORWARD && !startInclusive || direction == BACKWARD && startInclusive)) {
                // - direction == FORWARD && !startInclusive: If the search key is (10, 5) and there are
                //   rows (10, 5, ...) then we do not want them if !startInclusive. Making the search key
                //   (10, 5, AFTER) will cause these records to be skipped.
                // - direction == BACKWARD && startInclusive: Similarly, going in the other direction, we do the
                //   (10, 5, ...) records if startInclusive. But an LTEQ traversal would miss it unless we search
                //   for (10, 5, AFTER).
                startKey.append(Key.AFTER);
            }
            // Copy just the persistit key part of startKey to the exchange's key. startKey may be overspecified.
            // E.g., if we have a PK index for a non-root table, the index row is [childPK, parentPK], and an index
            // scan may specify a value for both. But the persistit search can only deal with the [childPK] part of
            // the traversal.
            startKey.copyPersistitKeyTo(exchange.getKey());
            pastStart = false;
        }
    }

    private IndexCursorUnidirectional(QueryContext context,
                                      IterationHelper iterationHelper,
                                      API.Ordering ordering,
                                      SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        super(context, iterationHelper, sortKeyAdapter, null);
        this.ordering = ordering;
        if (ordering.allAscending()) {
            this.startBoundary = Key.BEFORE;
            this.keyComparison = Key.GT;
            this.subsequentKeyComparison = Key.GT;
        } else if (ordering.allDescending()) {
            this.startBoundary = Key.AFTER;
            this.keyComparison = Key.LT;
            this.subsequentKeyComparison = Key.LT;
        } else {
            assert false : ordering;
        }
        this.startKey = null;
        this.endKey = null;
        this.startBoundColumns = 0;
        this.endBoundColumns = 0;
        this.sortKeyAdapter = sortKeyAdapter;
    }

    // Object state

    private boolean pastStart;
}
