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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.PersistitKeyPValueTarget;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

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
            Key key = exchange.getKey();
            key.clear();
            
            // Temporary until we remove old type system
            if (Types3Switch.ON) pTarget.attach(key); 
            else target.attach(key);
            
            IndexRowComposition irc = groupIndex.indexRowComposition();

            for(int i=0, LEN = irc.getLength(); i < LEN; ++i) {
                assert irc.isInRowData(i);
                assert ! irc.isInHKey(i);

                final int flattenedIndex = irc.getFieldPosition(i);
                Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);

                if (Types3Switch.ON) {
                    PValueSource source = row.pvalue(flattenedIndex);
                    TInstance sourceInstance = row.rowType().typeInstanceAt(flattenedIndex);
                    TClass sourceClass = sourceInstance.typeClass();
                    sourceClass.writeCollating(source, sourceInstance, pTarget.expectingType(column));
                }
                else {
                    ValueSource source = row.eval(flattenedIndex);
                    Converters.convert(source, target.expectingType(column));
                }
            }
            // The group index row's value contains a bitmap indicating which of the tables covered by the index
            // have rows contributing to this index row. The leafmost table of the index is represented by bit position 0.
            exchange.getValue().clear();
            exchange.getValue().put(tableBitmap(groupIndex, row));

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

    private static long tableBitmap(GroupIndex groupIndex, Row row) {
        long result = 0;
         int indexFromEnd = 0;
         for(UserTable table=groupIndex.leafMostTable(), END=groupIndex.rootMostTable().parentTable();
                !(table == null || table.equals(END));
                table = table.parentTable()
        ){
            if (row.containsRealRowOf(table)) {
                result |= 1 << table.getDepth();
            }
            ++indexFromEnd;
        }
        return result;
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
    private final PersistitKeyPValueTarget pTarget = new PersistitKeyPValueTarget();
    
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
