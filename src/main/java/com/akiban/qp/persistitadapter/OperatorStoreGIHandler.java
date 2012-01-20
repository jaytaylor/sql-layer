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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.types.conversion.Converters;
import com.akiban.util.ArgumentValidation;
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
        Key key = exchange.getKey();
        key.clear();
        target.attach(key);
        IndexRowComposition irc = groupIndex.indexRowComposition();

        for(int i=0, LEN = irc.getLength(); i < LEN; ++i) {
            assert irc.isInRowData(i);
            assert ! irc.isInHKey(i);

            final int flattenedIndex = irc.getFieldPosition(i);
            Column column = groupIndex.getColumnForFlattenedRow(flattenedIndex);

            ValueSource source = row.eval(flattenedIndex);
            Converters.convert(source, target.expectingType(column));
        }

        // Description of group index entry values:
        // The value of each index key is the depth of the leafmost table for which there is an actual row.
        // For instance, if you have a COI group and a group index on (i.sku, o.date), LEFT JOIN semantics mean
        // that an order with no items will have an index key of (null, <date>, <hkey>) with value = depth(o) == 1.
        // If you then give that order an item with a null sku, the index key will still be
        // (null, <date>, <hkey) but its value will be depth(i) == 2.
        // This depth is stored as an int (we don't anticipate any group branches with depth > 2^31-1).
        long realTablesMap = realTablesMap(groupIndex, row);
        exchange.getValue().clear();
        exchange.getValue().put(realTablesMap);

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
            if (exchange.fetch().getValue().isDefined()) { // see bug 914044 for why we can't do if(exchange.remove())
                exchange.remove();
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

    private static long realTablesMap(GroupIndex groupIndex, Row row) {
        long result = 0;
         int indexFromEnd = 0;
         for(UserTable table=groupIndex.leafMostTable(), END=groupIndex.rootMostTable().parentTable();
                !(table == null || table.equals(END));
                table = table.parentTable()
        ){
            if (row.containsRealRowOf(table)) {
                result |= 1 << indexFromEnd;
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
    
    // class state
    private static volatile GIHandlerHook giHandlerHook;
    private static final Tap.PointTap UNNEEDED_DELETE_TAP = Tap.createCount("superfluous_delete");

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
