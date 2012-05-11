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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.*;
import com.akiban.qp.row.Row;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;

import java.util.BitSet;

class OperatorStoreGIHandler {

    // GroupIndexHandler interface

    public void handleRow(GroupIndex groupIndex, Row row, Action action)
    {
        GroupIndexPosition sourceRowPosition = positionWithinBranch(groupIndex, sourceTable);
        if (sourceRowPosition.equals(GroupIndexPosition.BELOW_SEGMENT)) { // asserts sourceRowPosition != null :-)
            return; // nothing to do
        }

        Exchange exchange = adapter.takeExchange(groupIndex);
        try {
            prepareIndexRow(groupIndex, row, exchange);
            switch (action) {
            case STORE:
                storeExchange(groupIndex, exchange);
                break;
            case DELETE:
                removeExchange(groupIndex, exchange);
                break;
            default:
                throw new UnsupportedOperationException(action.name());
            }
        } finally {
            adapter.returnExchange(exchange);
        }
    }

    // class interface

    public static OperatorStoreGIHandler forTable(PersistitAdapter adapter, UserTable userTable) {
        ArgumentValidation.notNull("userTable", userTable);
        return new OperatorStoreGIHandler(adapter, userTable);
    }

    public static OperatorStoreGIHandler forBuilding(PersistitAdapter adapter) {
        return new OperatorStoreGIHandler(adapter, null);
    }

    // For use within the package

    static void setGiHandlerHook(GIHandlerHook newHook) {
        OperatorStoreGIHandler.giHandlerHook = newHook;
    }

    // for use in this class

    private void storeExchange(GroupIndex groupIndex, Exchange exchange) {
        try {
            exchange.store();
            AccumulatorAdapter.updateAndGet(AccumulatorAdapter.AccumInfo.ROW_COUNT, exchange, 1);
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
        if (giHandlerHook != null) {
            giHandlerHook.storeHook(groupIndex, exchange.getKey(), exchange.getValue().get());
        }
    }

    private void removeExchange(GroupIndex groupIndex, Exchange exchange) {
        try {
            if (exchange.remove()) {
                AccumulatorAdapter.updateAndGet(AccumulatorAdapter.AccumInfo.ROW_COUNT, exchange, -1);
            }
            else
                UNNEEDED_DELETE_TAP.hit();
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
        if (giHandlerHook != null) {
            giHandlerHook.removeHook(groupIndex, exchange.getKey());
        }
    }

    private void prepareIndexRow(GroupIndex groupIndex, Row row, Exchange exchange)
    {
        Key key = exchange.getKey();
        Value value = exchange.getValue();
        key.clear();
        value.clear();
        // Ancestor bitmap
        long ancestorBitmap = ancestorBitmap(groupIndex, row);
        value.put(ancestorBitmap);
        // Declared columns, leafward hkey columns, rootward hkey columns
        int groupIndexColumns = groupIndex.getAllColumns().size();
        target.attach(key);
        GroupIndexRowComposition irc = groupIndex.groupIndexRowComposition();
        int fields = irc.size();
        // copiedFromSource[f] will be set to true for columns written from source, (i.e., target not set to null).
        BitSet copiedFromSource = new BitSet(fields);
        for (int f = 0; f < fields; f++) {
            int flattenedIndex = irc.positionInFlattenedRow(f);
            Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);
            target.expectingType(column);
            // Write a null if the column comes from a missing row
            if ((ancestorBitmap & (1 << column.getUserTable().getDepth())) == 0) {
                // No source row, so write a null
                target.putNull();
            } else {
                // We have a source row
                boolean needToCopySource;
                if (f <= groupIndexColumns) {
                    // declared column or leafward hkey column
                    needToCopySource = true;
                } else {
                    // We're working on a rootward hkey column. We can avoid actually storing the value if some
                    // hkey-equivalent column already has written a non-null value. (If the row is present, as
                    // indicated by the ancestor bitmap, then a reader knows to consult that equivalent column.
                    // If the row is missing, we've already written a null and the reader will see that null.)
                    int[] equivalentHKeyColumnPositions = irc.equivalentHKeyIndexPositions(f);
                    assert equivalentHKeyColumnPositions != null;
                    needToCopySource = true;
                    for (int equivalentHKeyColumnPosition : equivalentHKeyColumnPositions) {
                        if (copiedFromSource.get(equivalentHKeyColumnPosition)) {
                            needToCopySource = false;
                        }
                    }
                    if (!needToCopySource) {
                        target.putNull();
                    }
                }
                if (needToCopySource) {
                    ValueSource source = row.eval(flattenedIndex);
                    Converters.convert(source, target);
                    copiedFromSource.set(f, true);
                }
            }
        }
    }

    // Return a bitmap indicating what ancestors of the indexed row are present. Bit i represents the table whose
    // depth is i. 1 means the row is present, 0 means the row is missing.
    private static long ancestorBitmap(GroupIndex groupIndex, Row row)
    {
        long bitmap = 0;
        UserTable table = groupIndex.leafMostTable();
        int indexRootDepth = groupIndex.rootMostTable().getDepth();
        while (table != null && table.getDepth() >= indexRootDepth) {
            if (row.containsRealRowOf(table)) {
                bitmap |= 1 << table.getDepth();
            }
            table = table.parentTable();
        }
        return bitmap;
    }

    private static GroupIndexPosition positionWithinBranch(GroupIndex groupIndex, UserTable table) {
        final UserTable leafMost = groupIndex.leafMostTable();
        if (table == null) {
            return GroupIndexPosition.ABOVE_SEGMENT;
        }
        else if (table.equals(leafMost)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        else if (table.isDescendantOf(leafMost)) {
            return GroupIndexPosition.BELOW_SEGMENT;
        }
        else if (groupIndex.rootMostTable().equals(table)) {
            return GroupIndexPosition.WITHIN_SEGMENT;
        }
        else {
            return groupIndex.rootMostTable().isDescendantOf(table)
                    ? GroupIndexPosition.ABOVE_SEGMENT
                    : GroupIndexPosition.WITHIN_SEGMENT;
        }
    }

    private OperatorStoreGIHandler(PersistitAdapter adapter, UserTable sourceTable) {
        this.adapter = adapter;
        this.sourceTable = sourceTable;
    }

    // object state

    private final PersistitAdapter adapter;
    private final UserTable sourceTable;
    private final PersistitKeyValueTarget target = new PersistitKeyValueTarget();
    
    // class state
    private static volatile GIHandlerHook giHandlerHook;
    private static final PointTap UNNEEDED_DELETE_TAP = Tap.createCount("superfluous_delete");

    // nested classes

    interface GIHandlerHook {
        void storeHook(GroupIndex groupIndex, Key key, Object value);
        void removeHook(GroupIndex groupIndex, Key key);
    }

    enum GroupIndexPosition {
        ABOVE_SEGMENT,
        BELOW_SEGMENT,
        WITHIN_SEGMENT
    }

    static enum Action {STORE, DELETE }
}
